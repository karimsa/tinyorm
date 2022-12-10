import {
	Pool as PostgresPool,
	PoolConfig as PostgresPoolOptions,
	PoolClient as PostgresClient,
} from "pg";
import { EntityFromShape, getEntityFields } from "./entity";
import {
	finalizeQuery,
	joinAllQueries,
	PostgresValueType,
	sql,
} from "./queries";

export class Connection {
	constructor(readonly client: PostgresClient) {}

	async createNewTable(entity: EntityFromShape<unknown>) {
		return this.client.query(
			finalizeQuery(ConnectionPool.getCreateTableQuery(entity, true)),
		);
	}

	async createTable(entity: EntityFromShape<unknown>) {
		return this.client.query(
			finalizeQuery(ConnectionPool.getCreateTableQuery(entity, false)),
		);
	}

	async insertOne<Shape>(entity: EntityFromShape<Shape>, entry: Shape) {
		return this.client.query(
			finalizeQuery(ConnectionPool.getInsertQuery(entity, entry)),
		);
	}
}

export class ConnectionPool {
	constructor(readonly clientPool: PostgresPool) {}

	async withClient<T>(fn: (client: PostgresClient) => Promise<T>): Promise<T> {
		const client = await this.clientPool.connect();

		try {
			return await fn(client);
		} finally {
			client.release();
		}
	}

	async withTransaction<T>(
		fn: (connection: Connection) => Promise<T>,
		isolationLevel:
			| "SERIALIZABLE"
			| "REPEATABLE READ"
			| "READ COMMITTED"
			| "READ UNCOMMITTED" = "READ COMMITTED",
	): Promise<T> {
		const client = await this.clientPool.connect();
		await client.query(`BEGIN TRANSACTION ISOLATION LEVEL ${isolationLevel}`);

		const connection = new Connection(client);

		try {
			const res = await fn(connection);
			await client.query(`COMMIT`);
			client.release();

			return res;
		} catch (err) {
			await client.query(`ROLLBACK`);
			client.release();

			throw err;
		}
	}

	static getCreateTableQuery<Shape>(
		entity: EntityFromShape<Shape>,
		mustBeNew?: boolean,
	) {
		const fieldSet = getEntityFields(entity);
		if (fieldSet.size === 0) {
			throw new Error(
				`Cannot perform a CREATE TABLE on an entity with no fields`,
			);
		}

		const columns = [...fieldSet.entries()];

		return joinAllQueries([
			sql`CREATE TABLE ${sql.asUnescaped(
				mustBeNew ? "" : "IF NOT EXISTS",
			)} ${sql.getEntityRef(entity)}`,
			sql.brackets(
				joinAllQueries(
					columns.map(([column, options], index) =>
						sql.unescaped(
							`"${column}" ${options.type} ${
								options.nullable ? "" : "NOT NULL"
							} ${index === columns.length - 1 ? "" : ", "}`,
						),
					),
				),
			),
		]);
	}

	static getInsertQuery<Shape>(entity: EntityFromShape<Shape>, entry: Shape) {
		const fieldSet = getEntityFields(entity);
		if (fieldSet.size === 0) {
			throw new Error(`Cannot perform an insert on an entity with no fields`);
		}

		const columns = [...fieldSet.keys()];

		return joinAllQueries([
			sql`INSERT INTO ${sql.getEntityRef(entity)}`,
			sql.brackets(
				sql.unescaped(columns.map((column) => `"${column}"`).join(", ")),
			),
			sql.unescaped(`VALUES`),
			sql.brackets(
				joinAllQueries(
					columns.map(
						(column, index) =>
							sql`${
								entry[column] as unknown as PostgresValueType
							}${sql.asUnescaped(index === columns.length - 1 ? "" : ", ")}`,
					),
				),
			),
		]);
	}
}

export async function createConnectionPool(options: PostgresPoolOptions) {
	const pgClientPool = new PostgresPool(options);
	const pool = new ConnectionPool(pgClientPool);
	await pool.withClient(async (client) => {
		await client.query(`select true`);
	});
	return pool;
}

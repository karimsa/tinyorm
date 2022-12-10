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

	getInsertQuery<Shape>(entity: EntityFromShape<Shape>, entry: Shape) {
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

	async insertOne<Shape>(entity: EntityFromShape<Shape>, entry: Shape) {
		return this.withClient(async (client) => {
			return client.query(finalizeQuery(this.getInsertQuery(entity, entry)));
		});
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

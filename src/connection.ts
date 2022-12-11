import {
	Pool as PostgresPool,
	PoolConfig as PostgresPoolOptions,
	PoolClient as PostgresClient,
} from "pg";
import {
	Column,
	Entity,
	EntityFromShape,
	getEntityFields,
	getEntityIndices,
} from "./entity";
import {
	FinalizedQuery,
	finalizeQuery,
	joinAllQueries,
	PostgresValueType,
	sql,
} from "./queries";
import { createJoinBuilder } from "./query-builder";

class TableColumnSchema extends Entity({
	schema: "information_schema",
	tableName: "columns",
}) {
	@Column({ type: 'text' })
	table_schema: string;
	@Column({type:'text'})
	table_name: string;
	@Column({type:'text'})
	column_name: string;
	@Column({type:'text'})
	is_nullable: string;
	@Column({type:'text'})
	column_default: string;
	@Column({type:'text'})
	data_type: string;
}

export type MigrationReason = "Missing Table" | "Missing Index";

export interface SuggestedMigration {
	reason: MigrationReason;
	query: FinalizedQuery;
}

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

	async dropTable(entity: EntityFromShape<unknown>) {
		return this.client.query(
			finalizeQuery(sql`DROP TABLE IF EXISTS ${entity}`),
		);
	}

	async insertOne<Shape>(entity: EntityFromShape<Shape>, entry: Shape) {
		return this.client.query(
			finalizeQuery(ConnectionPool.getInsertQuery(entity, entry)),
		);
	}

	async getMigrationQueries(
		entity: EntityFromShape<unknown>,
	): Promise<SuggestedMigration[]> {
		const migrationQueries: SuggestedMigration[] = [];

		const existingColumnData = await createJoinBuilder()
			.from(TableColumnSchema, "col")
			.selectAll("col")
			.where((where) =>
				where("col", "table_schema")
					.Equals(entity.schema)
					.andWhere("col", "table_name")
					.Equals(entity.tableName),
			)
			.getMany(this.client);

		// Creation of table from scratch
		if (existingColumnData.length === 0) {
			migrationQueries.push({
				reason: "Missing Table",
				query: finalizeQuery(ConnectionPool.getCreateTableQuery(entity, false)),
			});

			for (const query of getEntityIndices(entity).values()) {
				migrationQueries.push({
					reason: "Missing Index",
					query,
				});
			}
		}

		return migrationQueries;
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

	async getMigrationQueries(entity: EntityFromShape<unknown>) {
		return this.withTransaction(async (connection) => {
			return connection.getMigrationQueries(entity);
		});
	}

	destroy() {
		return this.clientPool.end();
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
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
	Index,
} from "./entity";
import { debug } from "./logger";
import {
	FinalizedQuery,
	finalizeQuery,
	joinAllQueries,
	PostgresValueType,
	sql,
} from "./queries";
import { createJoinBuilder, QueryError } from "./query-builder";
import {
	SchemaCatalog,
	TableCatalog,
	TableColumnCatalog,
	TableIndexCatalog,
} from "./pgcatalog";

@Index(Migrations)('idx_migration_name', ['name'], {unique: true})
class Migrations extends Entity({
	schema: "tinyorm",
	tableName: "migrations",
}) {
	@Column({ type: 'text' })
	readonly name: string;
	@Column({ type: 'timestamp without time zone' })
	readonly started_at: Date;
	@Column({ type: 'timestamp without time zone', nullable: true })
	readonly completed_at: Date | null;
}

export type MigrationReason =
	| "Missing Schema"
	| "Missing Table"
	| "Missing Index"
	| "Unused Index"
	| "New Index"
	| "Index Updated";

export interface SuggestedMigration {
	reason: MigrationReason;
	queries: FinalizedQuery[];
}

// rome-ignore lint/suspicious/noExplicitAny: This is a type-guard
function isSuggestedMigration(migration: any): migration is SuggestedMigration {
	return (
		typeof migration === "object" &&
		migration !== null &&
		typeof migration.reason === "string" &&
		Array.isArray(migration.queries)
	);
}

export class DuplicateMigrationError extends Error {
	constructor(readonly migrationName: string) {
		super(
			`Cannot rerun migration: migration with name '${migrationName}' has previously completed`,
		);
	}
}

export class Connection {
	constructor(readonly client: PostgresClient) {}

	async query(query: FinalizedQuery) {
		if (!query.text) {
			throw new Error(`Cannot run empty query`);
		}

		const startTime = Date.now();

		try {
			debug("query", `Running query`, { query });
			const res = await this.client.query(query);
			debug("query", `Query completed`, {
				query,
				duration: Date.now() - startTime,
			});
			return res;
		} catch (err) {
			debug("errors", `Query failed`, {
				query,
				err,
				duration: Date.now() - startTime,
			});
			throw new QueryError(
				err instanceof Error ? err.message : "Query failed",
				query,
				err,
			);
		}
	}

	async createNewTable(entity: EntityFromShape<unknown>) {
		return this.query(
			finalizeQuery(ConnectionPool.getCreateTableQuery(entity, true)),
		);
	}

	async createTable(entity: EntityFromShape<unknown>) {
		return this.query(
			finalizeQuery(ConnectionPool.getCreateTableQuery(entity, false)),
		);
	}

	async dropTable(entity: EntityFromShape<unknown>) {
		return this.query(finalizeQuery(sql`DROP TABLE IF EXISTS ${entity}`));
	}

	async insertOne<Shape>(entity: EntityFromShape<Shape>, entry: Shape) {
		return this.query(
			finalizeQuery(ConnectionPool.getInsertQuery(entity, entry)),
		);
	}

	async getMigrationQueries(
		entity: EntityFromShape<unknown>,
	): Promise<SuggestedMigration[]> {
		const migrationQueries: SuggestedMigration[] = [];

		const schemaInfo = await createJoinBuilder()
			.from(SchemaCatalog, "schema_entry")
			.selectAll("schema_entry")
			.where((where) =>
				where("schema_entry", "schema_name").Equals(entity.schema),
			)
			.getOne(this.client);

		// Creation of schema
		if (!schemaInfo) {
			migrationQueries.push({
				reason: "Missing Schema",
				queries: [
					finalizeQuery(
						sql`CREATE SCHEMA IF NOT EXISTS "${sql.asUnescaped(
							entity.schema,
						)}"`,
					),
				],
			});
		}

		// Creation of table from scratch
		const tableInfo = await createJoinBuilder()
			.from(TableCatalog, "table_entry")
			.selectAll("table_entry")
			.where((where) =>
				where("table_entry", "table_schema")
					.Equals(entity.schema)
					.andWhere("table_entry", "table_name")
					.Equals(entity.tableName),
			)
			.getOne(this.client);
		if (!tableInfo) {
			migrationQueries.push({
				reason: "Missing Table",
				queries: [
					finalizeQuery(ConnectionPool.getCreateTableQuery(entity, false)),
				],
			});

			for (const query of getEntityIndices(entity).values()) {
				migrationQueries.push({
					reason: "Missing Index",
					queries: [query],
				});
			}
			return migrationQueries;
		}

		// Identify index changes
		const indexSet = getEntityIndices(entity);
		const existingIndexData = await createJoinBuilder()
			.from(TableIndexCatalog, "index_entry")
			.selectAll("index_entry")
			.where((where) =>
				where("index_entry", "schemaname")
					.Equals(entity.schema)
					.andWhere("index_entry", "tablename")
					.Equals(entity.tableName),
			)
			.getMany(this.client);

		for (const index of existingIndexData) {
			const currentIndex = indexSet.get(index.index_entry.indexname);
			if (currentIndex) {
				if (index.index_entry.indexdef !== currentIndex.text) {
					migrationQueries.push({
						reason: "Index Updated",
						queries: [
							finalizeQuery(
								sql`DROP INDEX IF EXISTS "${sql.asUnescaped(
									entity.schema,
								)}"."${sql.asUnescaped(index.index_entry.indexname)}"`,
							),
							currentIndex,
						],
					});
				}
			} else {
				// Indices that need to be dropped
				migrationQueries.push({
					reason: "Unused Index",
					queries: [
						finalizeQuery(
							sql`DROP INDEX IF EXISTS "${sql.asUnescaped(
								entity.schema,
							)}"."${sql.asUnescaped(index.index_entry.indexname)}"`,
						),
					],
				});
			}
		}
		for (const [indexName, indexQuery] of indexSet.entries()) {
			if (
				!existingIndexData.find(
					(row) => row.index_entry.indexname === indexName,
				)
			) {
				migrationQueries.push({
					reason: "New Index",
					queries: [indexQuery],
				});
			}
		}

		const existingColumnData = await createJoinBuilder()
			.from(TableColumnCatalog, "col")
			.selectAll("col")
			.where((where) =>
				where("col", "table_schema")
					.Equals(entity.schema)
					.andWhere("col", "table_name")
					.Equals(entity.tableName),
			)
			.getMany(this.client);

		// TODO: Generate column migrations

		return migrationQueries;
	}

	async unsafe_resetAllMigrations() {
		try {
			await this.query(
				finalizeQuery(sql`DELETE FROM ${Migrations} WHERE TRUE`),
			);
		} catch (err) {
			if (!String(err).match(/relation.*does not exist/)) {
				throw err;
			}
		}
	}

	async executeMigration(
		name: string,
		queries: (FinalizedQuery | SuggestedMigration)[],
	) {
		// Auto-migrate the migrations table
		for (const { queries } of await this.getMigrationQueries(Migrations)) {
			for (const query of queries) {
				await this.query(query);
			}
		}

		// Create migration entry to avoid duplicate runs
		try {
			await this.insertOne(Migrations, {
				name,
				started_at: new Date(),
				completed_at: null,
			});
		} catch (err) {
			if (
				!String(err).includes(
					'duplicate key value violates unique constraint "idx_migration_name"',
				)
			) {
				throw err;
			}
			throw new DuplicateMigrationError(name);
		}

		// Run all queries
		for (const query of queries) {
			if (isSuggestedMigration(query)) {
				for (const subQuery of query.queries) {
					await this.query(subQuery);
				}
			} else {
				await this.query(query);
			}
		}
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

	async executeMigration(
		name: string,
		queries: (FinalizedQuery | SuggestedMigration)[],
	) {
		return this.withTransaction(async (connection) => {
			return connection.executeMigration(name, queries);
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
							}${index === columns.length - 1 ? "" : ", "}`,
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
			sql`INSERT INTO ${sql.getEntityRef(entity)} `,
			sql.brackets(
				sql.unescaped(columns.map((column) => `"${column}"`).join(", ")),
			),
			sql.unescaped(` VALUES `),
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

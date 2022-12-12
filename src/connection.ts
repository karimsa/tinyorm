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
import { MigrationGenerator, SuggestedMigration } from "./migrations";
import {
	createSingleWhereBuilder,
	SingleWhereQueryBuilder,
	WhereQueryBuilder,
} from "./where-builder";

export class QueryError extends Error {
	constructor(
		message: string,
		private readonly query: FinalizedQuery,
		private readonly internalError: unknown,
	) {
		super(message);
	}
}

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
			const { rows, rowCount } = await this.client.query(query);
			debug("query", `Query completed`, {
				query,
				duration: Date.now() - startTime,
				rowCount,
			});
			return rows;
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

	async deleteFrom<Shape extends object>(
		entity: EntityFromShape<Shape>,
		whereBuilder: (where: SingleWhereQueryBuilder<Shape>) => WhereQueryBuilder,
	) {
		return this.query(
			finalizeQuery(ConnectionPool.getDeleteFromQuery(entity, whereBuilder)),
		);
	}

	async getMigrationQueries(entity: EntityFromShape<unknown>) {
		return new MigrationGenerator(this).getMigrationQueries(entity);
	}

	async initMigrations() {
		for (const { queries } of await this.getMigrationQueries(Migrations)) {
			for (const query of queries) {
				await this.query(query);
			}
		}
	}

	async unsafe_resetAllMigrations() {
		try {
			await this.deleteFrom(Migrations, (where) => where.raw(sql`true`));
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
								(entry as Record<string, PostgresValueType>)[column]
							}${sql.asUnescaped(index === columns.length - 1 ? "" : ", ")}`,
					),
				),
			),
		]);
	}

	static getDeleteFromQuery<Shape extends object>(
		entity: EntityFromShape<Shape>,
		whereBuilder: (where: SingleWhereQueryBuilder<Shape>) => WhereQueryBuilder,
	) {
		const whereQuery = whereBuilder(
			createSingleWhereBuilder(entity),
		).getQuery();
		return sql`DELETE FROM ${entity} ${whereQuery}`;
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

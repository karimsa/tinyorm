import snakeCase from "lodash.snakecase";
import {
	Client as PgClientBase,
	Pool as PostgresPool,
	PoolClient as PostgresClient,
	PoolConfig as PostgresPoolOptions,
} from "pg";
import { EventEmitter } from "stream";
import {
	Column,
	Entity,
	EntityForeignKey,
	EntityFromShape,
	getEntityFields,
	getEntityKeys,
	Index,
} from "./entity";
import { createInsertBuilder } from "./insert-builder";
import { debug } from "./logger";
import { MigrationGenerator, SuggestedMigration } from "./migrations";
import { FinalizedQuery, isPreparedQuery, PreparedQuery, sql } from "./queries";
import { createEventEmitter, TypeSafeEventEmitter } from "./utils";
import {
	createSingleWhereBuilder,
	SingleWhereQueryBuilder,
	WhereQueryBuilder,
} from "./where-builder";

/**
 * This error is thrown when a query fails.
 */
export class QueryError extends Error {
	/**
	 * The query that failed.
	 */
	readonly query: FinalizedQuery;

	/**
	 * The error that was caught by TinyORM when the query failed.
	 */
	readonly internalError: unknown;

	constructor(message: string, query: FinalizedQuery, internalError: unknown) {
		super(message);

		this.query = query;
		this.internalError = internalError;
	}
}

@Index(Migrations)("idx_migration_name", ["name"], { unique: true })
class Migrations extends Entity({
	schema: "tinyorm",
	tableName: "migrations",
}) {
	@Column({ type: "text" })
	readonly name!: string;
	@Column({ type: "timestamp without time zone" })
	readonly started_at!: Date;
	@Column({ type: "timestamp without time zone", nullable: true })
	readonly completed_at!: Date | null;
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

/**
 * Thrown when a migration has already been run previously.
 */
export class DuplicateMigrationError extends Error {
	constructor(readonly migrationName: string) {
		super(
			`Cannot rerun migration: migration with name '${migrationName}' has previously completed`,
		);
	}
}

type ConnectionEvents = {
	queryStarted: { query: FinalizedQuery };
	queryFailed: { query: FinalizedQuery; error: QueryError };
	queryCompleted: { query: FinalizedQuery; duration: number };
	migrationStarted: {
		name: string;
		queries: (FinalizedQuery | SuggestedMigration)[];
	};
	migrationFailed: {
		name: string;
		queries: (FinalizedQuery | SuggestedMigration)[];
		error: unknown;
	};
	migrationCompleted: {
		name: string;
		queries: (FinalizedQuery | SuggestedMigration)[];
		duration: number;
	};
};

/**
 * Connections wrap a Postgres client and provide utils specifically for tinyorm.
 */
export class Connection
	extends EventEmitter
	implements TypeSafeEventEmitter<ConnectionEvents>
{
	private readonly events = createEventEmitter<ConnectionEvents>();

	constructor(readonly client: PostgresClient) {
		super();
	}

	/**
	 * Executes any FinalizedQuery and returns the resulting rows.
	 * @param query any FinalizedQuery object
	 * @returns set of resulting rows (not validated)
	 */
	async query(
		query: FinalizedQuery | PreparedQuery,
	): Promise<{ rows: unknown[]; rowCount: number }> {
		if (isPreparedQuery(query)) {
			return this.query(sql.finalize(query));
		}

		if (!query.text) {
			throw new Error(`Cannot run empty query`);
		}

		const startTime = Date.now();
		this.emit("queryStarted", { query });

		try {
			debug("query", `Running query`, { query });
			const { rows, rowCount } = await this.client.query(query);
			const duration = Date.now() - startTime;

			this.emit("queryCompleted", { query, duration });
			debug("query", `Query completed`, {
				query,
				duration,
				rowCount,
			});

			return { rows: rows as unknown[], rowCount };
		} catch (err) {
			debug("errors", `Query failed`, {
				query,
				err,
				duration: Date.now() - startTime,
			});
			this.emit("queryFailed", { query, error: err });
			throw new QueryError(
				err instanceof Error ? err.message : "Query failed",
				query,
				err,
			);
		}
	}

	/**
	 * Creates a new table for the given entity, and fails if a table already exists.
	 * @param entity any tinyorm entity
	 * @returns promise that resolves with void when the table has been created
	 */
	async createNewTable(entity: EntityFromShape<unknown>) {
		await this.query(ConnectionPool.getCreateTableQuery(entity, true));
	}

	/**
	 * Creates a new table for the given entity, if it doesn't already exist.
	 * @param entity any tinyorm entity
	 * @returns promise that resolves with void when the table has been created
	 */
	async createTable(entity: EntityFromShape<unknown>) {
		await this.query(ConnectionPool.getCreateTableQuery(entity, false));
	}

	/**
	 * Drops the table for the given entity, if it exists.
	 * @param entity any tinyorm entity
	 * @returns promise that resolves with void when the table has been dropped
	 */
	async dropTable(
		entity: EntityFromShape<unknown>,
		{ cascade }: { cascade?: boolean } = {},
	) {
		await this.query(
			sql`DROP TABLE IF EXISTS ${entity} ${sql.asUnescaped(
				cascade ? "CASCADE" : "",
			)}`,
		);
	}

	/**
	 * Deletes multiple rows from the given entity's table that match the given whereBuilder.
	 * @param entity any tinyorm entity
	 * @param whereBuilder function that returns a WhereQueryBuilder to select specific entity rows
	 * @returns the number of rows that were deleted
	 */
	async deleteFrom<Shape extends object>(
		entity: EntityFromShape<Shape>,
		whereBuilder: (where: SingleWhereQueryBuilder<Shape>) => WhereQueryBuilder,
	) {
		const { rowCount } = await this.query(
			ConnectionPool.getDeleteFromQuery(entity, whereBuilder),
		);
		return rowCount;
	}

	/**
	 * Compares the current entity definition with the table in the database and returns a list of
	 * queries that need to be run to bring the table in sync with the entity definition.
	 * @param entity any tinyorm entity
	 * @returns a set of suggested migrations recommended to bring the table in sync with the entity
	 */
	async getMigrationQueries(entity: EntityFromShape<unknown>) {
		return new MigrationGenerator(this).getMigrationQueries(entity);
	}

	/**
	 * Synchronizes an entity's database state with the entity's definition. Useful for testing and development
	 * environments. For production, it is recommended that you checkin the migrations to source control.
	 *
	 * @param entity any tinyorm entity
	 */
	async synchronizeEntity(entity: EntityFromShape<unknown>) {
		for (const query of await this.getMigrationQueries(entity)) {
			for (const subQuery of query.queries) {
				await this.query(subQuery);
			}
		}
	}

	/**
	 * Synchronizes the migrations table so migrations can be run. This should be called before any
	 * migrations are run.
	 */
	async initMigrations() {
		for (const { queries } of await this.getMigrationQueries(Migrations)) {
			for (const query of queries) {
				await this.query(query);
			}
		}
	}

	/**
	 * Destroys all record of previously run migrations. Exists to support testing, which is why it's
	 * marked as unsafe.
	 */
	async unsafe_resetAllMigrations() {
		try {
			await this.deleteFrom(Migrations, (where) => where.raw(sql`true`));
		} catch (err) {
			if (!String(err).match(/relation.*does not exist/)) {
				throw err;
			}
		}
	}

	/**
	 * Runs a migration, and fails if a migration with the same name has already run.
	 *
	 * Note: the migrations table must be synchronized before this method is called. To do this,
	 * please see [`Connection.initMigrations`](#initmigrations-method).
	 *
	 * @param name the name of the migration to run (recorded in the migrations table)
	 * @param queries set of queries considered to be part of the migration
	 */
	async executeMigration(
		name: string,
		queries: (FinalizedQuery | SuggestedMigration)[],
	) {
		// Create migration entry to avoid duplicate runs
		try {
			await createInsertBuilder(Migrations)
				.addRows([
					{
						name,
						started_at: new Date(),
						completed_at: null,
					},
				])
				.execute(this);
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

		this.emit("migrationStarted", { name, queries });

		try {
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
			this.emit("migrationCompleted", { name, queries });
		} catch (err) {
			this.emit("migrationFailed", { name, queries, error: err });
			throw err;
		}
	}
}

/**
 * Wraps a pool of postgres clients and provides utility methods.
 *
 * To create a connection pool, use the utility method `createConnectionPool`.
 */
export class ConnectionPool {
	/**
	 * The underlying postgres client pool. Direct access to this is discouraged, but is provided.
	 */
	readonly clientPool: PostgresPool;

	constructor(clientPool: PostgresPool) {
		this.clientPool = clientPool;
	}

	/**
	 * Borrows a client from the pool, and executes a function with exclusive access to that client.
	 * This exists only for lower level use cases, where you need to access the client directly. You
	 * usually want 'withConnection'.
	 *
	 * ```ts
	 * await pool.withClient(async (client) => {
	 * 	  // do whatever you want with the underlying client
	 * });
	 * ```
	 */
	async withClient<T>(fn: (client: PostgresClient) => Promise<T>): Promise<T> {
		const client = await this.clientPool.connect();

		try {
			return await fn(client);
		} finally {
			client.release();
		}
	}

	/**
	 * Executes a function with exclusive access to a connection outside of a transaction.
	 * This is meant to be used to execute queries, run migrations, etc.
	 *
	 * ```ts
	 * await pool.withConnection(async (connection) => {
	 * 	  // do whatever you want with the connection
	 * });
	 * ```
	 */
	async withConnection<T>(
		fn: (connection: Connection) => Promise<T>,
	): Promise<T> {
		const client = await this.clientPool.connect();

		try {
			return await fn(new Connection(client));
		} finally {
			client.release();
		}
	}

	/**
	 * Similar to 'withConnection', but executes the function inside a transaction.
	 * Transaction finalization (commit/rollback) is handled automatically based on whether the function throws an error.
	 *
	 * ```ts
	 * await pool.withTransaction(async (tx) => {
	 * 	  // do whatever you want with the 'tx' as a connection
	 * });
	 * ```
	 *
	 * @param fn the function to execute inside the transaction
	 * @param isolationLevel the transaction isolation level to use
	 * @returns the same value returned by your function
	 */
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

	/**
	 * Compares the current entity definition with the table in the database and returns a list of
	 * queries that need to be run to bring the table in sync with the entity definition.
	 *
	 * @param entity any tinyorm entity
	 * @returns a set of suggested migrations recommended to bring the table in sync with the entity
	 */
	async getMigrationQueries(entity: EntityFromShape<unknown>) {
		return this.withTransaction(async (connection) => {
			return connection.getMigrationQueries(entity);
		});
	}

	/**
	 * Runs a migration, and fails if it has already been run.
	 * @param name the name of the migration to run (recorded in the migrations table)
	 * @param queries set of queries considered to be part of the migration
	 */
	async executeMigration(
		name: string,
		queries: (FinalizedQuery | SuggestedMigration)[],
	) {
		return this.withTransaction(async (connection) => {
			return connection.executeMigration(name, queries);
		});
	}

	/**
	 * Closes the connection pool, and all of its clients.
	 * @returns
	 */
	async destroy() {
		await this.clientPool.end();
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

		const keySet = getEntityKeys(entity);
		const primaryKey = keySet?.find((key) => key.type === "primary");
		const foreignKeys = keySet?.filter(
			(key): key is EntityForeignKey => key.type === "foreign",
		);
		const columns = [...fieldSet.entries()];

		return sql`
			CREATE TABLE ${sql.asUnescaped(
				mustBeNew ? "" : "IF NOT EXISTS",
			)} ${sql.getEntityRef(entity)}
			${sql.brackets(
				sql`${sql.join(
					columns.map(([column, options], index) =>
						sql.join([
							sql.unescaped(`"${column}" `),
							sql.unescaped(`${options.type} `),
							sql.unescaped(options.nullable ? "" : "NOT NULL "),
							options.defaultValue
								? sql`DEFAULT ${options.defaultValue}`
								: sql``,
							sql.unescaped(index === columns.length - 1 ? "" : ", "),
						]),
					),
				)}

				${
					primaryKey
						? sql.unescaped(`,\nPRIMARY KEY ("${primaryKey.columnName}")`)
						: sql``
				}
				${
					foreignKeys && Number(foreignKeys.length) > 0
						? sql.unescaped(
								",\n" +
									foreignKeys
										.map(
											(key) => `
										CONSTRAINT fk_${snakeCase(
											`${key.refEntity.tableName}_${key.refColumn}`,
										)}
										FOREIGN KEY ("${key.columnName}")
										REFERENCES ${sql.getEntityRef(key.refEntity).value} ("${
												key.refColumn
											}")
									`,
										)
										.join(",\n"),
						  )
						: sql``
				}
				`,
			)}
		`;
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

/**
 * Creates a new connection pool, with exclusive access to an underlying postgres client pool.
 *
 * @param options takes the same options as [pg.Pool](https://node-postgres.com/features/pooling)
 */
export function createConnectionPool(
	options: PostgresPoolOptions,
): ConnectionPool {
	// @ts-ignore
	return new ConnectionPool(new PostgresPool(options));
}

function isPostgresClient(client: unknown): client is PostgresClient {
	return client instanceof PgClientBase;
}

export function wrapClient(client: PostgresClient | Connection): Connection {
	if (isPostgresClient(client)) {
		return new Connection(client);
	}
	return client;
}

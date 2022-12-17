import {
	Pool as PostgresPool,
	PoolClient as PostgresClient,
	PoolConfig as PostgresPoolOptions,
} from "pg";
import { EventEmitter } from "stream";
import {
	Column,
	Entity,
	EntityFromShape,
	getEntityFields,
	Index,
} from "./entity";
import { debug } from "./logger";
import { MigrationGenerator, SuggestedMigration } from "./migrations";
import {
	FinalizedQuery,
	finalizeQuery,
	PostgresValueType,
	sql,
} from "./queries";
import { createEventEmitter, TypeSafeEventEmitter } from "./utils";
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

@Index(Migrations)("idx_migration_name", ["name"], { unique: true })
class Migrations extends Entity({
	schema: "tinyorm",
	tableName: "migrations",
}) {
	@Column({ type: "text" })
	readonly name: string;
	@Column({ type: "timestamp without time zone" })
	readonly started_at: Date;
	@Column({ type: "timestamp without time zone", nullable: true })
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
	async query(query: FinalizedQuery): Promise<unknown[]> {
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

			return rows;
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
		await this.query(
			finalizeQuery(ConnectionPool.getCreateTableQuery(entity, true)),
		);
	}

	/**
	 * Creates a new table for the given entity, if it doesn't already exist.
	 * @param entity any tinyorm entity
	 * @returns promise that resolves with void when the table has been created
	 */
	async createTable(entity: EntityFromShape<unknown>) {
		return this.query(
			finalizeQuery(ConnectionPool.getCreateTableQuery(entity, false)),
		);
	}

	/**
	 * Drops the table for the given entity, if it exists.
	 * @param entity any tinyorm entity
	 * @returns promise that resolves with void when the table has been dropped
	 */
	async dropTable(entity: EntityFromShape<unknown>) {
		return this.query(finalizeQuery(sql`DROP TABLE IF EXISTS ${entity}`));
	}

	/**
	 * Inserts a single row into the given entity's table.
	 * @param entity any tinyorm entity
	 * @param entry an instance of the entity
	 * @returns the entity as returned by the database
	 */
	async insertOne<Shape>(entity: EntityFromShape<Shape>, entry: Shape) {
		return this.query(
			finalizeQuery(ConnectionPool.getInsertQuery(entity, entry)),
		);
	}

	/**
	 * Deletes multiple rows from the given entity's table that match the given whereBuilder.
	 * @param entity any tinyorm entity
	 * @param whereBuilder function that returns a WhereQueryBuilder to select specific entity rows
	 * @returns
	 */
	async deleteFrom<Shape extends object>(
		entity: EntityFromShape<Shape>,
		whereBuilder: (where: SingleWhereQueryBuilder<Shape>) => WhereQueryBuilder,
	) {
		return this.query(
			finalizeQuery(ConnectionPool.getDeleteFromQuery(entity, whereBuilder)),
		);
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
	 * Synchronizes an entity's database state with the entity's definition.
	 * @param entity any tinyorm entity
	 */
	async synchronizeEntity(entity: EntityFromShape<unknown>) {
		await this.initMigrations();

		for (const query of await this.getMigrationQueries(entity)) {
			for (const subQuery of query.queries) {
				await this.query(subQuery);
			}
		}
	}

	/**
	 * Synchronizes the migrations table so migrations can be run.
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
	 * Runs a migration, and fails if it has already been run.
	 * @param name the name of the migration to run (recorded in the migrations table)
	 * @param queries set of queries considered to be part of the migration
	 */
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

/**
 * Wraps a pool of postgres clients and provides utility methods.
 *
 * To create a connection pool, use the utility method `createConnectionPool`.
 */
export class ConnectionPool {
	constructor(readonly clientPool: PostgresPool) {}

	/**
	 * Borrows a client from the pool, and executes a function with exclusive access to that client.
	 * @param fn
	 * @returns
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
	 * Borrows a client from the pool, and executes a function with exclusive access to that client, under a transaction.
	 * Transaction finalization (commit/rollback) is handled automatically based on whether the function throws an error.
	 *
	 * @param fn
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

		const columns = [...fieldSet.entries()];

		return sql`
			CREATE TABLE ${sql.asUnescaped(
				mustBeNew ? "" : "IF NOT EXISTS",
			)} ${sql.getEntityRef(entity)}
			${sql.brackets(
				sql.join(
					columns.map(([column, options], index) =>
						sql.unescaped(
							`"${column}" ${options.type} ${
								options.nullable ? "" : "NOT NULL"
							}${index === columns.length - 1 ? "" : ", "}`,
						),
					),
				),
			)}
		`;
	}

	static getInsertQuery<Shape>(entity: EntityFromShape<Shape>, entry: Shape) {
		const fieldSet = getEntityFields(entity);
		if (fieldSet.size === 0) {
			throw new Error(`Cannot perform an insert on an entity with no fields`);
		}

		const columns = [...fieldSet.keys()];

		return sql`
			INSERT INTO ${sql.getEntityRef(entity)}
			${sql.brackets(
				sql.unescaped(columns.map((column) => `"${column}"`).join(", ")),
			)}
			VALUES
			${sql.brackets(
				sql.join(
					columns.map(
						(column, index) =>
							sql`${
								(entry as Record<string, PostgresValueType>)[column]
							}${sql.asUnescaped(index === columns.length - 1 ? "" : ", ")}`,
					),
				),
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
 * @param options
 * @returns
 */
export function createConnectionPool(options: PostgresPoolOptions) {
	return new ConnectionPool(new PostgresPool(options));
}

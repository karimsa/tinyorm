import { Connection, ConnectionPool, wrapClient } from "./connection";
import { EntityFromShape, getEntityFields } from "./entity";
import { PostgresValueType, PreparedQuery, sql } from "./queries";

import pick from "lodash.pick";
import { PoolClient as PostgresClient } from "pg";

export class InsertBuilder<
	Shape,
	InsertionKeys extends string & keyof Shape,
	ResultShape,
> {
	readonly #entity: EntityFromShape<Shape>;
	readonly #rows: Partial<Shape>[] = [];
	#insertionColumns: string[] = [];
	#returningColumns: string[] = [];

	constructor(
		entity: EntityFromShape<Shape>,
		insertionColumns: InsertionKeys[],
	) {
		this.#entity = entity;
		this.#insertionColumns = insertionColumns;

		const fieldSet = getEntityFields(this.#entity);
		if (fieldSet.size === 0) {
			throw new Error(`Cannot perform an insert on an entity with no fields`);
		}

		for (const column of this.#insertionColumns) {
			if (!fieldSet.has(column)) {
				throw new Error(
					`Cannot insert into unknown column "${column}" in entity "${this.#entity.name}"`,
				);
			}
		}
	}

	addRows(rows: Pick<Shape, InsertionKeys>[]) {
		for (const row of rows) {
			for (const column of this.#insertionColumns) {
				if (!{}.hasOwnProperty.call(row, column)) {
					throw new Error(
						`Cannot insert row, it is missing column "${column}"`,
					);
				}
			}
		}
		this.#rows.push(...(rows as unknown[] as Shape[]));

		return this;
	}

	returning<Column extends string & keyof Shape>(columns: Column[]) {
		this.#returningColumns = columns;
		return this as unknown as InsertBuilder<
			Shape,
			InsertionKeys,
			ResultShape & { [key in Column]: Shape[key] }
		>;
	}

	getPreparedQuery(): PreparedQuery {
		if (this.#rows.length === 0) {
			throw new Error(`Cannot insert zero rows`);
		}

		return sql`
			INSERT INTO ${this.#entity}
			${sql.brackets(
				sql.unescaped(
					this.#insertionColumns.map((column) => `"${column}"`).join(", "),
				),
			)}
			VALUES
			${sql.join(
				this.#rows.map((row) =>
					sql.brackets(
						sql.join(
							this.#insertionColumns.map(
								(column, index) =>
									sql`${
										(row as unknown as Record<string, unknown>)[
											column
										] as unknown as PostgresValueType
									}${sql.asUnescaped(
										index === this.#insertionColumns.length - 1 ? "" : ", ",
									)}`,
							),
						),
					),
				),
			)}
			${
				this.#returningColumns.length === 0
					? sql``
					: sql`RETURNING ${sql.brackets(
							sql.unescaped(
								this.#returningColumns.map((key) => `"${key}"`).join(", "),
							),
					  )}`
			}
		`;
	}

	getQuery() {
		return sql.finalize(this.getPreparedQuery());
	}

	async execute(
		connectionOrClient: ConnectionPool | Connection | PostgresClient,
	): Promise<ResultShape[]> {
		if (connectionOrClient instanceof ConnectionPool) {
			return connectionOrClient.withConnection((connection) => {
				return this.execute(connection);
			});
		}

		const connection = wrapClient(connectionOrClient);

		const { rows } = await connection.query(this.getQuery());
		return rows.map<ResultShape>(
			(row) => pick(row, this.#returningColumns) as unknown as ResultShape,
		);
	}
}

/**
 * Creates a new insert builder for the given entity which can be used to
 * insert rows into the database.
 *
 * ```ts
 * await createInsertBuilder()
 * 	.into(User)
 * 	.withColumns(["name", "age"])
 * 	.addRows([
 * 		{ name: "Alice", age: 20 },
 * 		{ name: "Bob", age: 21 },
 * 	])
 * 	.returning(["id", "name"])
 * 	.execute(connection);
 * ```
 *
 * @param entity the entity to insert rows into
 */
export function createInsertBuilder<Shape>(
	entity: EntityFromShape<Shape>,
): InsertBuilder<Shape, string & keyof Shape, {}>;
export function createInsertBuilder<
	Shape,
	InsertionKeys extends string & keyof Shape,
>(
	entity: EntityFromShape<Shape>,
	insertionColumns: InsertionKeys[],
): InsertBuilder<Shape, InsertionKeys, {}>;
export function createInsertBuilder<Shape>(
	entity: EntityFromShape<Shape>,
	insertionColumns?: (string & keyof Shape)[],
): InsertBuilder<Shape, string & keyof Shape, {}> {
	if (!insertionColumns) {
		const fieldSet = getEntityFields(entity);
		if (fieldSet.size === 0) {
			throw new Error(
				`Cannot create insert builder from entity with no fields`,
			);
		}
		return createInsertBuilder(entity, [
			...fieldSet.keys(),
		] as unknown[] as (string & keyof Shape)[]);
	}
	return new InsertBuilder(entity, insertionColumns);
}

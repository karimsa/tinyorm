import { Connection, ConnectionPool, wrapClient } from "./connection";
import { EntityFromShape, getEntityFields } from "./entity";
import { PostgresValueType, PreparedQuery, sql } from "./queries";

import pick from "lodash.pick";
import { PoolClient as PostgresClient } from "pg";

export class InsertBuilder<Shape, ResultShape> {
	readonly #entity: EntityFromShape<Shape>;
	readonly #rows: Shape[] = [];
	#returningColumns: string[] = [];

	constructor(entity: EntityFromShape<Shape>) {
		this.#entity = entity;
	}

	addRows(rows: Shape[]) {
		this.#rows.push(...rows);
		return this;
	}

	returning<Column extends string & keyof Shape>(columns: Column[]) {
		this.#returningColumns = columns;
		return this as unknown as InsertBuilder<
			Shape,
			ResultShape & { [key in Column]: Shape[key] }
		>;
	}

	getPreparedQuery(): PreparedQuery {
		const fieldSet = getEntityFields(this.#entity);
		if (fieldSet.size === 0) {
			throw new Error(`Cannot perform an insert on an entity with no fields`);
		}

		const columns = [...fieldSet.keys()];

		return sql`
			INSERT INTO ${this.#entity}
			${sql.brackets(
				sql.unescaped(columns.map((column) => `"${column}"`).join(", ")),
			)}
			VALUES
			${sql.join(
				this.#rows.map((row) =>
					sql.brackets(
						sql.join(
							columns.map(
								(column, index) =>
									sql`${
										(row as unknown as Record<string, unknown>)[
											column
										] as unknown as PostgresValueType
									}${sql.asUnescaped(
										index === columns.length - 1 ? "" : ", ",
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

export function createInsertBuilder<Shape>(entity: EntityFromShape<Shape>) {
	return new InsertBuilder(entity);
}

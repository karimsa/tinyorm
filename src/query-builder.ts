import { PoolClient as PostgresClient } from "pg";
import PostgresQueryCursor from "pg-cursor";
import { ZodSchema } from "zod";
import { Connection, QueryError } from "./connection";
import { EntityFromShape, getEntityFields } from "./entity";
import {
	FinalizedQuery,
	PostgresValueType,
	PreparedQuery,
	sql,
} from "./queries";
import { assertCase } from "./utils";
import {
	AndWhereQueryBuilder,
	createJoinWhereBuilder,
	createSingleWhereBuilder,
	JoinWhereQueryBuilder,
	OrWhereQueryBuilder,
	SingleWhereQueryBuilder,
} from "./where-builder";

/**
 * Pagination options that can be set on any query retrieval for how
 * many results to return and where to start from.
 */
export interface PaginationOptions {
	limit?: number;
	offset?: number;
}

function getPaginationQuery({
	limit,
	offset,
}: PaginationOptions = {}): PreparedQuery {
	return sql.join([
		offset === undefined ? sql`` : sql`OFFSET ${offset}`,
		limit === undefined ? sql`` : sql`LIMIT ${limit}`,
	]);
}

abstract class BaseQueryBuilder<ResultShape> {
	abstract buildOne(row: null): null;
	abstract buildOne(row: unknown): ResultShape;
	abstract getPreparedQuery(
		paginationOptions?: PaginationOptions,
	): PreparedQuery;

	getQuery(paginationOptions?: PaginationOptions): FinalizedQuery {
		return sql.finalize(this.getPreparedQuery(paginationOptions));
	}

	buildMany(rows: unknown[]): ResultShape[] {
		return rows
			.map((row) => this.buildOne(row))
			.filter((row): row is ResultShape => !!row);
	}

	private async executeQuery(
		client: PostgresClient | Connection,
		query: FinalizedQuery,
	) {
		if (client instanceof Connection) {
			return client.query(query);
		}

		try {
			return await client.query(query);
		} catch (err: unknown) {
			throw new QueryError(
				err instanceof Error ? String(err.message) : "Query failed",
				query,
				err,
			);
		}
	}

	async getOne(
		client: PostgresClient | Connection,
	): Promise<ResultShape | null> {
		const query = this.getQuery();
		query.text += " LIMIT 1";

		const { rows } = await this.executeQuery(client, query);
		if (rows.length === 0) {
			return null;
		}
		return this.buildOne(rows[0]);
	}

	async getOneOrFail(
		client: PostgresClient | Connection,
	): Promise<ResultShape> {
		const result = await this.getOne(client);
		if (!result) {
			throw new Error("Failed to find any results to query");
		}
		return result;
	}

	async getMany(
		client: PostgresClient | Connection,
		paginationOptions?: PaginationOptions,
	): Promise<ResultShape[]> {
		const query = this.getQuery(paginationOptions);
		const { rows } = await this.executeQuery(client, query);
		return this.buildMany(rows);
	}

	async *getCursor(
		clientOrConnection: PostgresClient | Connection,
		cursorOptions?: {
			initialOffset?: number;
			pageSize?: number;
		},
	): AsyncGenerator<Awaited<ResultShape>, void, undefined> {
		const pageSize = cursorOptions?.pageSize ?? 100;

		const client =
			clientOrConnection instanceof Connection
				? clientOrConnection.client
				: clientOrConnection;

		const query = this.getQuery();
		const cursor = client.query(
			new PostgresQueryCursor(query.text, query.values),
		);

		while (true) {
			const results = await cursor.read(pageSize);
			if (results.length === 0) {
				break;
			}

			for (const result of results) {
				yield this.buildOne(result);
			}
		}
	}
}

type ValidAlias<Alias, ReservedAliasRecord> = Alias &
	(Alias extends keyof ReservedAliasRecord
		? { invalid: "Cannot reuse an existing alias" }
		: {});

export class SimpleQueryBuilder<
	Shape extends object,
	ResultShape,
> extends BaseQueryBuilder<ResultShape> {
	readonly #selectedFields: string[] = [];
	readonly #selectedComputedFields = new Map<
		string,
		{ query: PreparedQuery; schema: ZodSchema<PostgresValueType> }
	>();
	readonly #orderByValues: PreparedQuery[] = [];
	readonly #groupByValues: PreparedQuery[] = [];
	readonly #whereQueries: PreparedQuery[] = [];
	readonly #targetFromEntity: EntityFromShape<Shape>;

	constructor(targetFromEntity: EntityFromShape<Shape>) {
		super();
		this.#targetFromEntity = targetFromEntity;
	}

	select<Keys extends string & keyof Shape>(
		keys: Keys[],
	): SimpleQueryBuilder<Shape, ResultShape & Pick<Shape, Keys>> {
		this.#selectedFields.push(...keys);
		return this as SimpleQueryBuilder<Shape, ResultShape & Pick<Shape, Keys>>;
	}

	selectAll() {
		for (const key of getEntityFields(this.#targetFromEntity).keys()) {
			if (!this.#selectedFields.includes(key)) {
				this.#selectedFields.push(key);
			}
		}
		return this as unknown as SimpleQueryBuilder<Shape, ResultShape & Shape>;
	}

	selectRaw<Alias extends string, T extends PostgresValueType>(
		query: PreparedQuery,
		alias: ValidAlias<Alias, ResultShape>,
		schema: ZodSchema<T>,
	) {
		this.#selectedComputedFields.set(alias, { query, schema });
		return this as unknown as SimpleQueryBuilder<
			Shape,
			ResultShape & { [key in Alias]: T }
		>;
	}

	addWhereRaw(query: PreparedQuery) {
		this.#whereQueries.push(query);
		return this;
	}

	addWhere(
		whereBuilder: (
			where: SingleWhereQueryBuilder<Shape>,
		) =>
			| AndWhereQueryBuilder<SingleWhereQueryBuilder<Shape>>
			| OrWhereQueryBuilder<SingleWhereQueryBuilder<Shape>>,
	) {
		this.#whereQueries.push(
			whereBuilder(
				createSingleWhereBuilder(this.#targetFromEntity),
			).getConditionQuery(),
		);
		return this;
	}

	/**
	 * Adds a custom raw query to the query's ORDER BY clause.
	 * @param query query that specifies a column & direction to order by
	 */
	addRawOrderBy(query: PreparedQuery) {
		this.#orderByValues.push(query);
		return this;
	}

	/**
	 * Adds a raw selection column to the query's ORDER BY clause.
	 *
	 * @param alias an alias for a raw field that was selected
	 * @param direction sort direction
	 */
	addOrderBy<Alias extends string & keyof ResultShape>(
		alias: Alias,
		direction: "ASC" | "DESC",
	): this;
	/**
	 * Adds a column from the selected entity to the query's ORDER BY clause.
	 *
	 * @param column a column name from the selected entity
	 * @param direction sort direction
	 */
	addOrderBy<Column extends string & keyof Shape,>(
		column: Column,
		direction: "ASC" | "DESC",
	): this;
	addOrderBy(aliasOrColumn: string, direction?: "ASC" | "DESC") {
		this.#orderByValues.push(sql.unescaped(`"${aliasOrColumn}" ${direction}`));
		return this;
	}

	#getOrderBy() {
		if (this.#orderByValues.length === 0) {
			return sql``;
		}

		const query = sql.join(
			this.#orderByValues.map((query, index, self) =>
				index === self.length - 1 ? query : sql.suffixQuery(query, ", "),
			),
		);
		return sql.prefixQuery(` ORDER BY `, query);
	}

	addRawGroupBy(query: PreparedQuery) {
		this.#groupByValues.push(query);
		return this;
	}

	addGroupBy<Column extends string & keyof Shape,>(column: Column) {
		this.#groupByValues.push(sql.unescaped(`"${column}"`));
		return this;
	}

	#getGroupBy() {
		if (this.#groupByValues.length === 0) {
			return sql``;
		}

		const query = sql.join(
			this.#groupByValues.map((query, index, self) =>
				index === self.length - 1 ? query : sql.suffixQuery(query, ", "),
			),
		);
		return sql.wrapQuery(` GROUP BY (`, query, `)`);
	}

	#lockingDirective: PreparedQuery = sql``;
	withLock(
		lockType:
			| "FOR UPDATE"
			| "FOR UPDATE NOWAIT"
			| "FOR UPDATE SKIP LOCKED"
			| "FOR SHARE"
			| "FOR SHARE NOWAIT"
			| "FOR KEY SHARE"
			| "FOR KEY SHARE NOWAIT"
			| "FOR NO KEY UPDATE",
	) {
		this.#lockingDirective = sql.unescaped(lockType);
		return this;
	}

	getPreparedQuery(paginationOptions?: PaginationOptions): PreparedQuery {
		if (this.#selectedFields.length === 0) {
			throw new Error(`No fields selected, cannot perform select query`);
		}

		return sql`
			SELECT ${sql.asUnescaped(
				this.#selectedFields.map((field) => `"${field}"`).join(", "),
			)}
			${this.#selectedComputedFields.size === 0 ? sql`` : sql`, `}
			${
				this.#selectedComputedFields.size === 0
					? sql``
					: sql.join(
							Array.from(this.#selectedComputedFields.entries()).flatMap(
								([alias, { query }], index, self) => [
									sql`${query} AS "${sql.asUnescaped(alias)}"`,
									index === self.length - 1 ? sql`` : sql`,`,
								],
							),
					  )
			}
			FROM ${this.#targetFromEntity}
			${
				this.#whereQueries.length === 0
					? sql``
					: sql.join(
							this.#whereQueries.flatMap((query, index) =>
								index === 0 ? [sql`WHERE `, query] : [sql` AND `, query],
							),
					  )
			}
			${this.#getGroupBy()}
			${this.#getOrderBy()}
			${getPaginationQuery(paginationOptions)}
			${this.#lockingDirective}
		`;
	}

	buildOne(row: null): null;
	buildOne(row: unknown): ResultShape;
	buildOne(row: unknown): ResultShape | null {
		if (row === null) {
			return null;
		}
		if (typeof row !== "object") {
			throw new Error("Unexpected row received in query result");
		}

		const resultBuilder: Record<string, unknown> = {};
		const castedRow = row as unknown as Record<string, unknown>;

		for (const field of this.#selectedFields) {
			const value = castedRow[field];
			resultBuilder[field] = value;
		}

		return resultBuilder as unknown as ResultShape;
	}
}

export class JoinedQueryBuilder<
	Shapes extends Record<string, object>,
	PartialShapes,
	ResultShape,
> extends BaseQueryBuilder<ResultShape> {
	readonly #selectedFields = new Map<string, string[]>();
	readonly #selectedComputedFields = new Map<
		string,
		{ query: PreparedQuery; schema: ZodSchema<unknown> }
	>();
	readonly #joins: PreparedQuery[] = [];
	readonly #includedEntites = new Map<string, EntityFromShape<unknown>>();
	readonly #orderByValues: PreparedQuery[] = [];
	readonly #groupByValues: PreparedQuery[] = [];
	readonly #whereQueries: PreparedQuery[] = [];
	readonly #targetFromEntity: EntityFromShape<unknown>;
	readonly #targetEntityAlias: string;

	constructor(
		targetFromEntity: EntityFromShape<unknown>,
		targetEntityAlias: string,
	) {
		super();
		this.#includedEntites.set(targetEntityAlias, targetFromEntity);
		this.#targetFromEntity = targetFromEntity;
		this.#targetEntityAlias = targetEntityAlias;
	}

	innerJoin<Alias extends string, JoinedShape>(
		joinedEntity: EntityFromShape<JoinedShape>,
		alias: ValidAlias<Alias, Shapes>,
		condition: PreparedQuery,
	) {
		assertCase("join alias", alias);
		if (this.#includedEntites.has(alias)) {
			throw new Error(
				`Cannot join two entities with same alias (attempted to alias ${joinedEntity.tableName} as ${alias})`,
			);
		}

		this.#includedEntites.set(alias, joinedEntity);
		this.#joins.push(
			sql` INNER JOIN ${sql.getEntityRef(joinedEntity, alias)} ON ${condition}`,
		);
		return this as unknown as JoinedQueryBuilder<
			Shapes & { [key in Alias]: JoinedShape },
			PartialShapes,
			ResultShape
		>;
	}

	leftJoin<Alias extends string, JoinedShape>(
		joinedEntity: EntityFromShape<JoinedShape>,
		alias: ValidAlias<Alias, Shapes>,
		condition: PreparedQuery,
	) {
		assertCase("join alias", alias);
		if (this.#includedEntites.has(alias)) {
			throw new Error(
				`Cannot join two entities with same alias (attempted to alias ${joinedEntity.tableName} as ${alias})`,
			);
		}

		this.#includedEntites.set(alias, joinedEntity);
		this.#joins.push(
			sql` LEFT JOIN ${sql.getEntityRef(joinedEntity, alias)} ON ${condition}`,
		);
		return this as unknown as JoinedQueryBuilder<
			Shapes & { [key in Alias]: JoinedShape },
			PartialShapes | Alias,
			ResultShape
		>;
	}

	rightJoin<Alias extends string, JoinedShape>(
		joinedEntity: EntityFromShape<JoinedShape>,
		alias: ValidAlias<Alias, Shapes>,
		condition: PreparedQuery,
	) {
		assertCase("join alias", alias);
		if (this.#includedEntites.has(alias)) {
			throw new Error(
				`Cannot join two entities with same alias (attempted to alias ${joinedEntity.tableName} as ${alias})`,
			);
		}

		this.#includedEntites.set(alias, joinedEntity);
		this.#joins.push(
			sql` RIGHT JOIN ${sql.getEntityRef(joinedEntity, alias)} ON ${condition}`,
		);
		return this as unknown as JoinedQueryBuilder<
			Shapes & { [key in Alias]: JoinedShape },
			PartialShapes | keyof Shapes,
			ResultShape
		>;
	}

	select<
		Alias extends string & keyof Shapes,
		Keys extends string & keyof Shapes[Alias],
	>(alias: Alias, keys: Keys[]) {
		const selectedFields = this.#selectedFields.get(alias) ?? [];
		this.#selectedFields.set(alias, selectedFields);
		selectedFields.push(...keys);

		return this as unknown as JoinedQueryBuilder<
			Shapes,
			PartialShapes,
			Alias extends PartialShapes
				? ResultShape & { [key in Alias]?: Pick<Shapes[Alias], Keys> }
				: ResultShape & { [key in Alias]: Pick<Shapes[Alias], Keys> }
		>;
	}

	selectAll<Alias extends string & keyof Shapes>(alias: Alias) {
		const entity = this.#includedEntites.get(alias);
		if (!entity) {
			throw new Error(`Unrecognized entity alias used: '${alias}'`);
		}

		const fieldSet = getEntityFields(entity as EntityFromShape<Shapes[Alias]>);

		const selectedFields = this.#selectedFields.get(alias) ?? [];
		this.#selectedFields.set(alias, selectedFields);
		selectedFields.push(...fieldSet.keys());

		return this as unknown as JoinedQueryBuilder<
			Shapes,
			PartialShapes,
			ResultShape & {
				[key in Alias]: Pick<Shapes[Alias], string & keyof Shapes[Alias]>;
			}
		>;
	}

	selectRaw<Alias extends string, T extends PostgresValueType>(
		query: PreparedQuery,
		alias: ValidAlias<Alias, Shapes>,
		schema: ZodSchema<T>,
	) {
		this.#selectedComputedFields.set(alias, { query, schema });
		return this as unknown as JoinedQueryBuilder<
			Shapes,
			PartialShapes,
			ResultShape & { [key in Alias]: T }
		>;
	}

	#getSelectedFields() {
		const selectedFields: string[] = [];
		for (const [entityName, fields] of this.#selectedFields.entries()) {
			selectedFields.push(
				...fields.map(
					(field) => `"${entityName}"."${field}" AS "${entityName}_${field}"`,
				),
			);
		}
		return selectedFields;
	}

	#getSelectedComputedFields() {
		const selectedFieldQueries: PreparedQuery[] = [];
		for (const [alias, { query }] of this.#selectedComputedFields.entries()) {
			selectedFieldQueries.push(sql.suffixQuery(query, ` AS "${alias}"`));
		}
		return selectedFieldQueries;
	}

	addWhereRaw(query: PreparedQuery) {
		this.#whereQueries.push(query);
		return this;
	}

	addWhere(
		whereBuilder: (
			where: JoinWhereQueryBuilder<Shapes>,
		) =>
			| AndWhereQueryBuilder<JoinWhereQueryBuilder<Shapes>>
			| OrWhereQueryBuilder<JoinWhereQueryBuilder<Shapes>>,
	) {
		this.#whereQueries.push(
			whereBuilder(createJoinWhereBuilder(this.#includedEntites)).getQuery(),
		);
		return this;
	}

	addRawOrderBy(query: PreparedQuery) {
		this.#orderByValues.push(query);
		return this;
	}

	addOrderBy<Alias extends string & keyof ResultShape>(
		alias: Alias,
		direction: "ASC" | "DESC",
	): this;
	addOrderBy<
		Alias extends string & keyof Shapes,
		Column extends string & keyof Shapes[Alias],
	>(alias: Alias, column: Column, direction: "ASC" | "DESC"): this;
	addOrderBy(
		alias: string,
		columnOrDirection: string,
		direction?: "ASC" | "DESC",
	) {
		if (direction) {
			this.#orderByValues.push(
				sql.unescaped(`"${alias}"."${columnOrDirection}" ${direction}`),
			);
		} else {
			this.#orderByValues.push(
				sql.unescaped(`"${alias}" ${columnOrDirection}`),
			);
		}
		return this;
	}

	#getOrderBy() {
		if (this.#orderByValues.length === 0) {
			return sql``;
		}

		const query = sql.join(
			this.#orderByValues.map((query, index, self) =>
				index === self.length - 1 ? query : sql.suffixQuery(query, ", "),
			),
		);
		return sql.prefixQuery(` ORDER BY `, query);
	}

	addRawGroupBy(query: PreparedQuery) {
		this.#groupByValues.push(query);
		return this;
	}

	addGroupBy<
		Alias extends string & keyof Shapes,
		Column extends string & keyof Shapes[Alias],
	>(alias: Alias, column: Column) {
		this.#groupByValues.push(sql.unescaped(`"${alias}"."${column}"`));
		return this;
	}

	#getGroupBy() {
		if (this.#groupByValues.length === 0) {
			return sql``;
		}

		const query = sql.join(
			this.#groupByValues.map((query, index, self) =>
				index === self.length - 1 ? query : sql.suffixQuery(query, ", "),
			),
		);
		return sql.wrapQuery(` GROUP BY (`, query, `)`);
	}

	#lockingDirective: PreparedQuery = sql``;
	withLock<Alias extends string & keyof Shapes>(
		alias: Alias | "ALL",
		lockType:
			| "FOR UPDATE"
			| "FOR UPDATE NOWAIT"
			| "FOR UPDATE SKIP LOCKED"
			| "FOR SHARE"
			| "FOR SHARE NOWAIT"
			| "FOR KEY SHARE"
			| "FOR KEY SHARE NOWAIT"
			| "FOR NO KEY UPDATE",
	) {
		this.#lockingDirective =
			alias === "ALL"
				? sql.unescaped(lockType)
				: sql.unescaped(`${lockType} OF "${alias}"`);
		return this;
	}

	getPreparedQuery(paginationOptions?: PaginationOptions): PreparedQuery {
		const selectedFields = this.#getSelectedFields();
		const selectedComputedFields = this.#getSelectedComputedFields();

		if (selectedFields.length === 0 && selectedComputedFields.length === 0) {
			throw new Error(`No fields selected, cannot perform select query`);
		}

		return sql`
			SELECT ${sql.asUnescaped(
				[
					...selectedFields,

					// Insert a trailing comma if computed fields will follow
					...(selectedComputedFields.length === 0 ? [] : [""]),
				].join(", "),
			)}
			${
				selectedComputedFields.length > 0
					? sql.join(selectedComputedFields)
					: sql``
			}
			FROM ${sql.getEntityRef(this.#targetFromEntity, this.#targetEntityAlias)}
			${this.#joins.length > 0 ? sql.join(this.#joins) : sql``}
			${this.#whereQueries.length > 0 ? sql.join(this.#whereQueries) : sql``}
			${this.#getGroupBy()}
			${this.#getOrderBy()}
			${getPaginationQuery(paginationOptions)}
			${this.#lockingDirective}
		`;
	}

	buildOne(row: null): null;
	buildOne(row: unknown): ResultShape;
	buildOne(row: unknown): ResultShape | null {
		if (row === null) {
			return null;
		}
		if (typeof row !== "object") {
			throw new Error("Unexpected row received in query result");
		}

		const resultBuilder: Record<string, unknown> = {};
		const castedRow = row as unknown as Record<string, unknown>;

		for (const [entityName, fields] of this.#selectedFields.entries()) {
			const subEntityResult = (resultBuilder[entityName] = {}) as Record<
				string,
				unknown
			>;

			for (const field of fields) {
				const value = castedRow[`${entityName}_${field}`];
				subEntityResult[field] = value;
			}
		}

		for (const [
			alias,
			{ query, schema },
		] of this.#selectedComputedFields.entries()) {
			const retrievedValue = castedRow[alias];
			const parseResult = schema.safeParse(retrievedValue);
			if (!parseResult.success) {
				throw new Error(
					`Query returned invalid value in column '${alias}' retreived by '${
						query.text
					}': ${parseResult.error.format()._errors[0] ?? "incorrect type"}`,
				);
			}

			resultBuilder[alias] = parseResult.data;
		}

		return resultBuilder as unknown as ResultShape;
	}
}

/**
 * Creates a new query builder that will select from a single entity.
 */
export function createSimpleQueryBuilder() {
	return {
		from<T extends object>(
			entity: EntityFromShape<T>,
		): SimpleQueryBuilder<T, {}> {
			return new SimpleQueryBuilder(entity);
		},
	};
}

/**
 * Creates a new query builder that will join multiple entities together.
 */
export function createJoinQueryBuilder() {
	return {
		from<Alias extends string, T extends object>(
			entity: EntityFromShape<T>,
			alias: Alias,
		): JoinedQueryBuilder<{ [key in Alias]: T }, never, {}> {
			return new JoinedQueryBuilder(entity, alias);
		},
	};
}

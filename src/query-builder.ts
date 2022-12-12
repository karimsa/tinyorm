import { PoolClient as PostgresClient } from "pg";
import { ZodSchema } from "zod";
import { Connection, QueryError } from "./connection";
import { EntityFromShape, getEntityFields } from "./entity";
import {
	FinalizedQuery,
	finalizeQuery,
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

abstract class BaseQueryBuilder<ResultShape> {
	abstract buildOne(row: unknown): ResultShape | null;
	abstract getPreparedQuery(): PreparedQuery;

	getQuery(): FinalizedQuery {
		return finalizeQuery(this.getPreparedQuery());
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
			const { rows } = await client.query(query);
			return rows;
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

		const rows = await this.executeQuery(client, query);
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

	async getMany(client: PostgresClient | Connection): Promise<ResultShape[]> {
		const query = this.getQuery();
		const rows = await this.executeQuery(client, query);
		return this.buildMany(rows);
	}
}

export class QueryBuilder<
	Shape extends object,
	ResultShape,
> extends BaseQueryBuilder<ResultShape> {
	readonly #selectedFields: string[] = [];
	readonly #orderByValues: PreparedQuery[] = [];
	readonly #groupByValues: PreparedQuery[] = [];
	readonly #whereQueries: PreparedQuery[] = [];

	constructor(readonly targetFromEntity: EntityFromShape<Shape>) {
		super();
	}

	select<Keys extends string & keyof Shape>(
		keys: Keys[],
	): QueryBuilder<Shape, ResultShape & Pick<Shape, Keys>> {
		this.#selectedFields.push(...keys);
		// rome-ignore lint/suspicious/noExplicitAny: <explanation>
		return this as any;
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
				createSingleWhereBuilder(this.targetFromEntity),
			).getConditionQuery(),
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

	#queryLimit?: number;
	limit(size: number) {
		if (this.#queryLimit !== undefined) {
			throw new Error(`Cannot set limit twice on the same query builder`);
		}
		this.#queryLimit = size;
		return this as Omit<QueryBuilder<Shape, ResultShape>, "limit">;
	}

	#queryOffset?: number;
	offset(offset: number) {
		if (this.#queryOffset !== undefined) {
			throw new Error(`Cannot set offset twice on the same query builder`);
		}
		this.#queryOffset = offset;
		return this as unknown as Omit<QueryBuilder<Shape, ResultShape>, "offset">;
	}

	getPreparedQuery(): PreparedQuery {
		if (this.#selectedFields.length === 0) {
			throw new Error(`No fields selected, cannot perform select query`);
		}

		return sql`
			SELECT ${sql.asUnescaped(
				this.#selectedFields.map((field) => `"${field}"`).join(", "),
			)}
			FROM ${this.targetFromEntity}
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
		`;
	}

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

type EntityAlias<Alias, Shapes extends Record<string, object>> = Alias &
	(Alias extends keyof Shapes
		? { invalid: "Cannot reuse an existing alias" }
		: {});

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

	constructor(
		readonly targetFromEntity: EntityFromShape<unknown>,
		readonly targetEntityAlias: string,
	) {
		super();
		this.#includedEntites.set(targetEntityAlias, targetFromEntity);
	}

	innerJoin<Alias extends string, JoinedShape>(
		joinedEntity: EntityFromShape<JoinedShape>,
		alias: EntityAlias<Alias, Shapes>,
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
		alias: EntityAlias<Alias, Shapes>,
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
		alias: EntityAlias<Alias, Shapes>,
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
		alias: EntityAlias<Alias, Shapes>,
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

	#queryLimit?: number;
	limit(size: number) {
		if (this.#queryLimit !== undefined) {
			throw new Error(`Cannot set limit twice on the same query builder`);
		}
		this.#queryLimit = size;
		return this as Omit<
			JoinedQueryBuilder<Shapes, PartialShapes, ResultShape>,
			"limit"
		>;
	}

	#queryOffset?: number;
	offset(offset: number) {
		if (this.#queryOffset !== undefined) {
			throw new Error(`Cannot set offset twice on the same query builder`);
		}
		this.#queryOffset = offset;
		return this as unknown as Omit<
			JoinedQueryBuilder<Shapes, PartialShapes, ResultShape>,
			"offset"
		>;
	}

	getPreparedQuery(): PreparedQuery {
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
			FROM ${sql.getEntityRef(this.targetFromEntity, this.targetEntityAlias)}
			${this.#joins.length > 0 ? sql.join(this.#joins) : sql``}
			${this.#whereQueries.length > 0 ? sql.join(this.#whereQueries) : sql``}
			${this.#getGroupBy()}
			${this.#getOrderBy()}
			${
				this.#queryOffset !== undefined
					? sql` OFFSET ${this.#queryOffset}`
					: sql``
			}
			${
				this.#queryLimit !== undefined ? sql` LIMIT ${this.#queryLimit}` : sql``
			}
		`;
	}

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

export function createSelectBuilder() {
	return {
		from<T extends object>(entity: EntityFromShape<T>): QueryBuilder<T, {}> {
			return new QueryBuilder(entity);
		},
	};
}

export function createJoinBuilder() {
	return {
		from<Alias extends string, T extends object>(
			entity: EntityFromShape<T>,
			alias: Alias,
		): JoinedQueryBuilder<{ [key in Alias]: T }, never, {}> {
			return new JoinedQueryBuilder(entity, alias);
		},
	};
}

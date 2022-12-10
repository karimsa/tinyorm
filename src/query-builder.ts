import { EntityFromShape } from "./entity";
import {
	FinalizedQuery,
	finalizeQuery,
	joinQueries,
	joinAllQueries,
	PreparedQuery,
	PostgresValueType,
	sql,
} from "./queries";
import { assertCase } from "./utils";
import { Pool as PostgresClientPool, PoolClient as PostgresClient } from "pg";
import {
	AndWhereQueryBuilder,
	createWhereBuilder,
	InternalWhereBuilder,
	OrWhereQueryBuilder,
	WhereQueryBuilder,
} from "./where-builder";
import { ZodSchema } from "zod";

export class QueryError extends Error {
	constructor(
		message: string,
		private readonly query: FinalizedQuery,
		private readonly internalError: unknown,
	) {
		super(message);
	}
}

abstract class BaseQueryBuilder<ResultShape> {
	abstract buildOne(row: unknown): ResultShape | null;
	abstract getQuery(): FinalizedQuery;

	buildMany(rows: unknown[]): ResultShape[] {
		return rows
			.map((row) => this.buildOne(row))
			.filter((row): row is ResultShape => !!row);
	}

	private async executeQuery(client: PostgresClient, query: FinalizedQuery) {
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

	async getOne(pool: PostgresClientPool): Promise<ResultShape | null> {
		const client = await pool.connect();

		const query = this.getQuery();
		query.text += " LIMIT 1";

		const rows = await this.executeQuery(client, query);
		client.release();
		return this.buildOne(rows[0]);
	}

	async getOneOrFail(pool: PostgresClientPool): Promise<ResultShape> {
		const result = await this.getOne(pool);
		if (!result) {
			throw new Error("Failed to find any results to query");
		}
		return result;
	}

	async getMany(pool: PostgresClientPool): Promise<ResultShape[]> {
		const client = await pool.connect();

		const query = this.getQuery();
		query.text += " LIMIT 1";

		const rows = await this.executeQuery(client, query);
		client.release();
		return this.buildMany(rows);
	}
}

class QueryBuilder<
	Shape extends object,
	ResultShape,
> extends BaseQueryBuilder<ResultShape> {
	readonly #selectedFields: string[] = [];

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

	getQuery(): FinalizedQuery {
		return {
			text: `
                SELECT ${this.#selectedFields
									.map((field) => `"${field}"`)
									.join(", ")}
                FROM ${sql.getEntityRef(this.targetFromEntity).value}
            `,
			values: [],
		};
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

class JoinedQueryBuilder<
	Shapes extends Record<string, object>,
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
	#whereBuilder: InternalWhereBuilder<Shapes> | null = null;

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
			joinQueries(
				sql` INNER JOIN ${sql.getEntityRef(joinedEntity, alias)} ON `,
				condition,
			),
		);
		return this as unknown as JoinedQueryBuilder<
			Shapes & { [key in Alias]: JoinedShape },
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
			ResultShape & { [key in Alias]: Pick<Shapes[Alias], Keys> }
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
			selectedFieldQueries.push({
				...query,
				text: [
					...query.text.slice(0, query.text.length - 1),
					`${query.text[query.text.length - 1]} AS "${alias}"`,
				],
			});
		}
		return selectedFieldQueries;
	}

	where(
		whereBuilder: (
			where: WhereQueryBuilder<Shapes>,
		) => AndWhereQueryBuilder<Shapes> | OrWhereQueryBuilder<Shapes>,
	) {
		this.#whereBuilder = whereBuilder(
			createWhereBuilder(this.#includedEntites),
		) as unknown as InternalWhereBuilder<Shapes>;
		return this as unknown as Omit<
			JoinedQueryBuilder<Shapes, ResultShape>,
			"where"
		>;
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

		const query = joinAllQueries(
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

		const query = joinAllQueries(
			this.#groupByValues.map((query, index, self) =>
				index === self.length - 1 ? query : sql.suffixQuery(query, ", "),
			),
		);
		return sql.wrapQuery(` GROUP BY (`, query, `)`);
	}

	getQuery(): FinalizedQuery {
		return finalizeQuery(
			joinAllQueries([
				sql` SELECT ${sql.asUnescaped(
					[
						...this.#getSelectedFields(),

						// Insert a trailing comma if computed fields will follow
						...(this.#selectedComputedFields.size === 0 ? [] : [""]),
					].join(", "),
				)} `,
				...this.#getSelectedComputedFields(),
				sql` FROM ${sql.getEntityRef(
					this.targetFromEntity,
					this.targetEntityAlias,
				)} `,
				...this.#joins,
				...(this.#whereBuilder ? [this.#whereBuilder.getQuery()] : []),
				this.#getGroupBy(),
				this.#getOrderBy(),
			]),
		);
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
		): JoinedQueryBuilder<{ [key in Alias]: T }, {}> {
			return new JoinedQueryBuilder(entity, alias);
		},
	};
}

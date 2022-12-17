import { EntityFromShape } from "./entity";
import {
	isJsonRef,
	JsonRef,
	PostgresBooleanColumnType,
	PostgresDateColumnType,
	PostgresNumericColumnType,
	PostgresSimpleValueType,
	PostgresStringColumnType,
	PostgresStringColumnTypes,
	PostgresValueType,
	PreparedQuery,
	readJsonRef,
	sql,
} from "./queries";
import { isElementOfArray } from "./utils";

type BaseWhereQueryComparators<T, NextQueryBuilder> = {
	// Misc
	Equals(value: T): NextQueryBuilder;
	NotEquals(value: T): NextQueryBuilder;

	// Cast helpers
	CastAs(
		castedType: PostgresBooleanColumnType,
	): WhereQueryComparators<boolean, NextQueryBuilder>;
	CastAs(
		castedType: PostgresNumericColumnType,
	): WhereQueryComparators<boolean, NextQueryBuilder>;
	CastAs(
		castedType: PostgresStringColumnType,
	): WhereQueryComparators<string, NextQueryBuilder>;
	CastAs(
		castedType: PostgresDateColumnType,
	): WhereQueryComparators<Date, NextQueryBuilder>;
	CastAs(castedType: string): WhereQueryComparators<unknown, NextQueryBuilder>;

	// Array
	EqualsAny(values: T[]): NextQueryBuilder;
	EqualsNone(values: T[]): NextQueryBuilder;

	// Text comparisons
	NotLike(values: string): NextQueryBuilder;
	Like(values: string): NextQueryBuilder;
};

export type WhereQueryComparators<T, NextQueryBuilder> =
	BaseWhereQueryComparators<T, NextQueryBuilder> & {
		// JSONB
		JsonContains(subObject: string | Partial<T>): NextQueryBuilder;
	};

export class InternalWhereBuilder<Shapes extends Record<string, object>> {
	#binaryOperator: "AND" | "OR" | null = null;
	readonly #queries: PreparedQuery[];
	readonly #knownEntities: Map<
		string & keyof Shapes,
		Pick<EntityFromShape<unknown>, "schema" | "tableName">
	>;

	constructor(
		knownEntities: Map<
			string & keyof Shapes,
			Pick<EntityFromShape<unknown>, "schema" | "tableName">
		>,
		queries: PreparedQuery[] = [],
	) {
		this.#knownEntities = knownEntities;
		this.#queries = queries;
	}

	getNextBuilder(queries: PreparedQuery[]) {
		return new InternalWhereBuilder(
			this.#knownEntities,
			queries,
		) as unknown as AndWhereQueryBuilder<
			ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>
		> &
			OrWhereQueryBuilder<
				ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>
			>;
	}

	getBuilder() {
		const that = this;
		function openWhere<
			Alias extends string & keyof Shapes,
			Key extends string & keyof Shapes[Alias],
		>(
			entityName: Alias,
			field: Key,
		): WhereQueryComparators<
			Shapes[Alias][Key],
			AndWhereQueryBuilder<
				ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>
			> &
				OrWhereQueryBuilder<
					ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>
				>
		>;
		function openWhere<Alias extends string & keyof Shapes>(
			entityName: Alias,
			field: JsonRef<Shapes[Alias]>,
		): WhereQueryComparators<
			object,
			AndWhereQueryBuilder<
				ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>
			> &
				OrWhereQueryBuilder<
					ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>
				>
		>;
		function openWhere(
			entityName: string | null,
			field: string | JsonRef<unknown>,
		): unknown {
			if (that.#binaryOperator || that.#queries.length > 0) {
				throw new Error("Cannot re-open where builder");
			}
			return that.openComparator(entityName, field) as unknown;
		}

		const allOrEither = (binaryOperator: "AND" | "OR") => {
			return (whereBuilders: { getConditionQuery(): PreparedQuery }[]) => {
				return this.getNextBuilder(
					whereBuilders.flatMap((builder, index) => [
						builder.getConditionQuery(),
						...(index === whereBuilders.length - 1
							? []
							: [sql.unescaped(` ${binaryOperator} `)]),
					]),
				);
			};
		};

		return Object.assign(openWhere, {
			all: allOrEither("AND"),
			either: allOrEither("OR"),
			raw: (query: PreparedQuery) => this.getNextBuilder([query]),
		});
	}

	andWhere<
		Alias extends string & keyof Shapes,
		Key extends string & keyof Shapes[Alias],
	>(
		entityName: Alias,
		field: Key | JsonRef<Shapes[Alias]>,
	): WhereQueryComparators<
		Shapes[Alias][Key],
		AndWhereQueryBuilder<ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>>
	>;
	andWhere<Alias extends string & keyof Shapes>(
		entityName: Alias,
		field: JsonRef<Shapes[Alias]>,
	): WhereQueryComparators<
		object,
		AndWhereQueryBuilder<ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>>
	>;
	andWhere(
		entityName: string | null,
		field: string | JsonRef<unknown>,
	): unknown {
		if (this.#binaryOperator === "OR") {
			throw new Error(
				`Cannot convert ${this.#binaryOperator} where builder into 'AND WHERE'`,
			);
		}

		this.#binaryOperator = "AND";
		return this.openComparator(entityName, field) as unknown;
	}

	orWhere<
		Alias extends string & keyof Shapes,
		Key extends string & keyof Shapes[Alias],
	>(
		entityName: Alias,
		field: Key | JsonRef<Shapes[Alias]>,
	): WhereQueryComparators<
		Shapes[Alias][Key],
		OrWhereQueryBuilder<ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>>
	>;
	orWhere<Alias extends string & keyof Shapes>(
		entityName: Alias,
		field: JsonRef<Shapes[Alias]>,
	): WhereQueryComparators<
		object,
		OrWhereQueryBuilder<ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>>
	>;
	orWhere(
		entityName: string | null,
		field: string | JsonRef<unknown>,
	): unknown {
		if (this.#binaryOperator === "AND") {
			throw new Error(
				`Cannot convert ${this.#binaryOperator} where builder into 'OR WHERE'`,
			);
		}
		this.#binaryOperator = "OR";
		return this.openComparator(entityName, field) as unknown;
	}

	#getColumnName(alias: string | null, field: string | JsonRef<unknown>) {
		if (isJsonRef(field)) {
			return alias
				? sql.asUnescaped(`"${alias}".${readJsonRef(field)}`)
				: sql.asUnescaped(readJsonRef(field));
		}
		return alias
			? sql.asUnescaped(`"${alias}"."${field}"`)
			: sql.asUnescaped(`"${field}"`);
	}

	#openBaseComparator(column: string) {
		const getColumnName = () => sql.asUnescaped(column);
		const comparators: WhereQueryComparators<
			PostgresValueType,
			AndWhereQueryBuilder<
				ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>
			> &
				OrWhereQueryBuilder<
					ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>
				>
		> = {
			Equals: (value) => this.#appendQuery(sql`${getColumnName()} = ${value}`),
			NotEquals: (value) =>
				this.#appendQuery(sql`${getColumnName()} != ${value}`),
			EqualsAny: (values: PostgresSimpleValueType[]) =>
				this.#appendQuery(sql`${getColumnName()} = ANY(${values})`),
			EqualsNone: (values: PostgresSimpleValueType[]) =>
				this.#appendQuery(sql`${getColumnName()} <> ANY(${values})`),
			Like: (value) => this.#appendQuery(sql`${getColumnName()} LIKE ${value}`),
			NotLike: (value) =>
				this.#appendQuery(sql`${getColumnName()} NOT LIKE ${value}`),
			JsonContains: (subObject: string | object) =>
				this.#appendQuery(
					sql`${getColumnName()} @> ${sql.asJSONB(
						typeof subObject === "string"
							? subObject
							: JSON.stringify(subObject),
					)}`,
				),
			CastAs: (castedType: string) => {
				const columnName = getColumnName().value;
				if (
					columnName.includes("->") &&
					!isElementOfArray(castedType, PostgresStringColumnTypes)
				) {
					// PG does not allow going straight into another type from jsonb, without first being a string
					return this.#openBaseComparator(
						`(${columnName})::text::${castedType}`,
					);
				}
				return this.#openBaseComparator(`(${columnName})::${castedType}`);
			},
		};
		return comparators;
	}

	openComparator<Alias extends string & keyof Shapes>(
		alias: Alias | null,
		field: string | JsonRef<unknown>,
	) {
		if (alias) {
			const entity = this.#knownEntities.get(alias);
			if (!entity) {
				throw new Error(
					`Unrecognized entity alias in where: ${alias} (expected one of: ${JSON.stringify(
						Array.from(this.#knownEntities.keys()),
					)})`,
				);
			}
		}
		return this.#openBaseComparator(
			this.#getColumnName(alias, field).value,
		) as unknown as WhereQueryComparators<
			unknown,
			AndWhereQueryBuilder<InternalWhereBuilder<Shapes>["getBuilder"]> &
				OrWhereQueryBuilder<InternalWhereBuilder<Shapes>["getBuilder"]>
		>;
	}

	#appendQuery(query: PreparedQuery) {
		if (this.#queries.length === 0) {
			return this.getNextBuilder([...this.#queries, query]);
		}
		return this.getNextBuilder([
			...this.#queries,
			sql.prefixQuery(this.#binaryOperator!, query),
		]);
	}

	getConditionQuery(): PreparedQuery {
		if (this.#queries.length === 1) {
			return this.#queries[0];
		}
		return sql.brackets(sql.join(this.#queries));
	}

	getQuery(): PreparedQuery {
		return sql` WHERE ${sql.join(this.#queries)}`;
	}
}

export class InternalSingleWhereBuilder<Shape extends object> {
	#internalBuilder: InternalWhereBuilder<{ default: Shape }>;

	constructor(
		targetEntity: EntityFromShape<Shape>,
		queries: PreparedQuery[] = [],
	) {
		this.#internalBuilder = new InternalWhereBuilder(
			new Map([["default", targetEntity]]),
			queries,
		);
		Object.assign(this.#internalBuilder, {
			getNextBuilder(queries: PreparedQuery[]) {
				return new InternalSingleWhereBuilder(targetEntity, queries);
			},
		});
	}

	getBuilder() {
		const builder = this.#internalBuilder.getBuilder();

		function openWhere<Key extends string & keyof Shape,>(
			field: Key,
		): WhereQueryComparators<
			Shape[Key],
			AndWhereQueryBuilder<SingleWhereQueryBuilder<Shape>> &
				OrWhereQueryBuilder<SingleWhereQueryBuilder<Shape>>
		>;
		function openWhere(
			field: JsonRef<Shape>,
		): WhereQueryComparators<
			object,
			AndWhereQueryBuilder<SingleWhereQueryBuilder<Shape>> &
				OrWhereQueryBuilder<SingleWhereQueryBuilder<Shape>>
		>;
		function openWhere(field: string | JsonRef<unknown>): unknown {
			return (builder as Function)(null, field);
		}

		return Object.assign(openWhere, {
			all(builders: InternalWhereBuilder<{ default: Shape }>[]) {
				return builder.all(builders);
			},
			either(builders: InternalWhereBuilder<{ default: Shape }>[]) {
				return builder.all(builders);
			},
			raw(query: PreparedQuery) {
				return builder.raw(query);
			},
		});
	}

	andWhere<Key extends string & keyof Shape,>(
		field: Key | JsonRef<Shape>,
	): WhereQueryComparators<
		Shape[Key],
		AndWhereQueryBuilder<SingleWhereQueryBuilder<Shape>>
	>;
	andWhere<Alias extends string & keyof Shape>(
		field: JsonRef<Shape>,
	): WhereQueryComparators<
		object,
		AndWhereQueryBuilder<SingleWhereQueryBuilder<Shape>>
	>;
	andWhere(field: string | JsonRef<unknown>): unknown {
		return (this.#internalBuilder.andWhere as Function)(null, field);
	}

	orWhere<Key extends string & keyof Shape,>(
		field: Key | JsonRef<Shape>,
	): WhereQueryComparators<
		Shape[Key],
		OrWhereQueryBuilder<SingleWhereQueryBuilder<Shape>>
	>;
	orWhere<Alias extends string & keyof Shape>(
		field: JsonRef<Shape>,
	): WhereQueryComparators<
		object,
		OrWhereQueryBuilder<SingleWhereQueryBuilder<Shape>>
	>;
	orWhere(field: string | JsonRef<unknown>): unknown {
		return (this.#internalBuilder.orWhere as Function)(null, field);
	}

	getConditionQuery() {
		return this.#internalBuilder.getConditionQuery();
	}

	getQuery() {
		return this.#internalBuilder.getQuery();
	}
}

export type JoinWhereQueryBuilder<Shapes extends Record<string, object>> =
	ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]> & {
		raw(
			query: PreparedQuery,
		): AndWhereQueryBuilder<JoinWhereQueryBuilder<Shapes>> &
			OrWhereQueryBuilder<JoinWhereQueryBuilder<Shapes>>;
	};
export type SingleWhereQueryBuilder<Shape extends object> = ReturnType<
	InternalSingleWhereBuilder<Shape>["getBuilder"]
> & {
	raw(
		query: PreparedQuery,
	): AndWhereQueryBuilder<SingleWhereQueryBuilder<Shape>> &
		OrWhereQueryBuilder<SingleWhereQueryBuilder<Shape>>;
};

export type AndWhereQueryBuilder<QueryBuilder extends Function> = {
	andWhere: QueryBuilder;
	getConditionQuery: () => PreparedQuery;
	getQuery: () => PreparedQuery;
};
export type OrWhereQueryBuilder<QueryBuilder extends Function> = {
	orWhere: QueryBuilder;
	getConditionQuery: () => PreparedQuery;
	getQuery: () => PreparedQuery;
};

export type WhereQueryBuilder =
	| AndWhereQueryBuilder<Function>
	| OrWhereQueryBuilder<Function>;

/**
 * Creates a simplified query builder that assembles the `where` part of a query for
 * a query that can only reference a single entity.
 */
export function createSingleWhereBuilder<Shape extends object>(
	entity: EntityFromShape<Shape>,
) {
	const builder = new InternalSingleWhereBuilder<Shape>(entity);
	return builder.getBuilder() as unknown as SingleWhereQueryBuilder<Shape>;
}

/**
 * Creates a query builder that assembles the `where` part of a query, assuming
 * that `knownEntities` are the only entities that can be referenced in the
 * query.
 *
 * @param knownEntities a map of entity names/aliases to their respective entity
 */
export function createJoinWhereBuilder<Shapes extends Record<string, object>>(
	knownEntities:
		| Map<
				string & keyof Shapes,
				Pick<EntityFromShape<unknown>, "schema" | "tableName">
		  >
		| Record<
				string & keyof Shapes,
				Pick<EntityFromShape<unknown>, "schema" | "tableName">
		  >,
): JoinWhereQueryBuilder<Shapes> {
	const builder = new InternalWhereBuilder<Shapes>(
		knownEntities instanceof Map
			? knownEntities
			: new Map(Object.entries(knownEntities)),
	);
	return builder.getBuilder();
}

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

export interface WhereQueryComparators<T, NextQueryBuilder> {
	// Misc
	/**
	 * Performs an exact match comparison.
	 *
	 * ```ts
	 * where('name').Equals('test')
	 * // Generates: WHERE name = 'test'
	 * ```
	 *
	 * @param value value to compare against
	 */
	Equals(value: T): NextQueryBuilder;
	/**
	 * Performs an exact non-match comparison.
	 *
	 * ```ts
	 * where('name').NotEquals('test')
	 * // Generates: WHERE name != 'test'
	 * ```
	 *
	 * @param value value to compare against
	 */
	NotEquals(value: T): NextQueryBuilder;

	// Cast helpers
	/**
	 * Casts the current column to a boolean.
	 *
	 * ```ts
	 * where('name').CastAs('boolean').Equals(true)
	 * // Generates: WHERE name::boolean = true
	 * ```
	 */
	CastAs(
		boolean: PostgresBooleanColumnType,
	): WhereQueryComparators<boolean, NextQueryBuilder>;
	/**
	 * Casts the current column to a number.
	 *
	 * ```ts
	 * where('name').CastAs('double precision').Equals(3.14)
	 * // Generates: WHERE name::double precision = 3.14
	 * ```
	 */
	CastAs(
		number: PostgresNumericColumnType,
	): WhereQueryComparators<boolean, NextQueryBuilder>;
	/**
	 * Casts the current column to a string.
	 *
	 * ```ts
	 * where('name').CastAs('text').Equals('test')
	 * // Generates: WHERE name::text = 'test'
	 * ```
	 */
	CastAs(
		string: PostgresStringColumnType,
	): WhereQueryComparators<string, NextQueryBuilder>;
	/**
	 * Casts the current column to a date or timestamp type.
	 *
	 * ```ts
	 * where('name').CastAs('date').Equals(new Date())
	 * // Generates: WHERE name::date = '2021-01-01'
	 * ```
	 */
	CastAs(
		date: PostgresDateColumnType,
	): WhereQueryComparators<Date, NextQueryBuilder>;
	/**
	 * Casts the current column to any other postgres type.
	 *
	 * ```ts
	 * where('name').CastAs('some_other_col_type').Equals('test')
	 * // Generates: WHERE name::some_other_col_type = 'test'
	 * ```
	 */
	CastAs(unknown: string): WhereQueryComparators<unknown, NextQueryBuilder>;

	// Array
	/**
	 * Performs an array contains comparison, checking to see if the given column
	 * contains any of the given values.
	 *
	 * ```ts
	 * where('name').EqualsAny(['test', 'test2'])
	 * // Generates: WHERE name = ANY(array{'test', 'test2'})
	 * ```
	 *
	 * @param values
	 */
	EqualsAny(values: T[]): NextQueryBuilder;
	/**
	 * Performs an array contains comparison, checking to see if the given column
	 * contains none of the given values.
	 *
	 * ```ts
	 * where('name').EqualsNone(['test', 'test2'])
	 * // Generates: WHERE name <> ANY(array{'test', 'test2'})
	 * ```
	 */
	EqualsNone(values: T[]): NextQueryBuilder;

	// Text comparisons
	/**
	 * Performs a partial text comparison.
	 *
	 * ```ts
	 * where('name').Like('%test%')
	 * // Generates: WHERE name LIKE '%test%'
	 * ```
	 */
	Like(values: string): NextQueryBuilder;
	/**
	 * Performs a partial text non-match comparison.
	 *
	 * ```ts
	 * where('name').NotLike('%test%')
	 * // Generates: WHERE name NOT LIKE '%test%'
	 * ```
	 */
	NotLike(values: string): NextQueryBuilder;

	// JSONB
	/**
	 * Performs a JSONB sub-object search comparison.
	 *
	 * ```ts
	 * where('data').JsonContains({ test: 'test' })
	 * // Generates: WHERE data @> '{"test": "test"}'
	 * ```
	 *
	 * See [postgres docs](https://www.postgresql.org/docs/9.5/functions-json.html) for more information.
	 */
	JsonContains(subObject: string | Partial<T>): NextQueryBuilder;
}

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

/**
 * Where query builder for a single entity, where `Shape` is the shape of the entity.
 *
 * ```ts
 * class User extends Entity({ tableName: 'users' }) {
 * 	readonly id: string;
 * 	readonly name: string;
 * }
 *
 * const where = createSingleWhereBuilder(User);
 *
 * // Generates: WHERE name = 'Karim'
 * const whereQuery = where('name').Equals('Karim').getQuery();
 * ```
 *
 * For possible comparators, see {@link WhereQueryComparators}.
 */
export type SingleWhereQueryBuilder<Shape extends object> = ReturnType<
	InternalSingleWhereBuilder<Shape>["getBuilder"]
> & {
	raw(
		query: PreparedQuery,
	): AndWhereQueryBuilder<SingleWhereQueryBuilder<Shape>> &
		OrWhereQueryBuilder<SingleWhereQueryBuilder<Shape>>;
};

/**
 * Where query builder where conditions can only be joined with `AND`.
 *
 * For possible comparators, see {@link WhereQueryComparators}.
 */
export interface AndWhereQueryBuilder<QueryBuilder extends Function> {
	andWhere: QueryBuilder;
	getConditionQuery: () => PreparedQuery;
	getQuery: () => PreparedQuery;
}

/**
 * Where query builder where conditions can only be joined with `OR`.
 *
 * For possible comparators, see {@link WhereQueryComparators}.
 */
export interface OrWhereQueryBuilder<QueryBuilder extends Function> {
	orWhere: QueryBuilder;
	getConditionQuery: () => PreparedQuery;
	getQuery: () => PreparedQuery;
}

/**
 * @internal
 */
export type WhereQueryBuilder =
	| AndWhereQueryBuilder<Function>
	| OrWhereQueryBuilder<Function>;

/**
 * Creates a simplified query builder that assembles the `where` part of a query for
 * a query that can only reference a single entity.
 *
 * ```ts
 * class User extends Entity({ tableName: 'users' }) {
 * 	readonly id: string;
 * 	readonly name: string;
 * }
 *
 * const where = createSingleWhereBuilder(User);
 *
 * // Generates: WHERE name = 'Karim'
 * const whereQuery = where('name').Equals('Karim').getQuery();
 * ```
 *
 * For possible comparators, see {@link WhereQueryComparators}.
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
 * ```ts
 * class User extends Entity({ tableName: 'users' }) {
 * 	readonly id: string;
 * 	readonly name: string;
 * }
 * class Post extends Entity({ tableName: 'posts' }) {
 * 	readonly id: string;
 * 	readonly text: string;
 *  readonly author_id: string;
 * }
 *
 * const where = createJoinWhereBuilder({
 * 	user: User,
 *  post: Post,
 * });
 *
 * // Generates: WHERE user.name = 'Karim' AND post.id = '1'
 * const whereQuery = where('user', 'name').Equals('Karim').andWhere('post', 'id').Equals('1').getQuery();
 * ```
 *
 * For possible comparators, see {@link WhereQueryComparators}.
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

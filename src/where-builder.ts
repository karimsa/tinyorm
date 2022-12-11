import { EntityFromShape } from "./entity";
import {
	PreparedQuery,
	PostgresValueType,
	sql,
	joinAllQueries,
	PostgresSimpleValueType,
	PostgresNumericColumnType,
	PostgresStringColumnType,
	PostgresBooleanColumnType,
	PostgresDateColumnType,
	PostgresStringColumnTypes,
	JsonRef,
	isJsonRef,
	readJsonRef,
} from "./queries";
import { isElementOfArray, JsonKeys } from "./utils";

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
			AndWhereQueryBuilder<Shapes> & OrWhereQueryBuilder<Shapes>
		>;
		function openWhere<Alias extends string & keyof Shapes>(
			entityName: Alias,
			field: JsonRef<Shapes[Alias]>,
		): WhereQueryComparators<
			object,
			AndWhereQueryBuilder<Shapes> & OrWhereQueryBuilder<Shapes>
		>;
		function openWhere(
			entityName: string,
			field: string | JsonRef<unknown>,
		): unknown {
			if (that.#binaryOperator || that.#queries.length > 0) {
				throw new Error("Cannot re-open where builder");
			}
			return that.#openComparator(entityName, field) as unknown;
		}

		return Object.assign(openWhere, {
			all: this.allOrEither("AND"),
			either: this.allOrEither("OR"),
		});
	}

	allOrEither(binaryOperator: "AND" | "OR") {
		return (
			whereBuilders: (
				| AndWhereQueryBuilder<Shapes>
				| OrWhereQueryBuilder<Shapes>
			)[],
		): InternalWhereBuilder<Shapes> => {
			const builders =
				whereBuilders as unknown[] as InternalWhereBuilder<Shapes>[];
			return new InternalWhereBuilder(
				this.#knownEntities,
				builders.flatMap((builder, index) => [
					builder.getConditionQuery(),
					...(index === builders.length - 1
						? []
						: [sql.unescaped(` ${binaryOperator} `)]),
				]),
			);
		};
	}

	andWhere<
		Alias extends string & keyof Shapes,
		Key extends string & keyof Shapes[Alias],
	>(
		entityName: Alias,
		field: Key | JsonRef<Shapes[Alias]>,
	): WhereQueryComparators<Shapes[Alias][Key], AndWhereQueryBuilder<Shapes>>;
	andWhere<Alias extends string & keyof Shapes>(
		entityName: Alias,
		field: JsonRef<Shapes[Alias]>,
	): WhereQueryComparators<object, AndWhereQueryBuilder<Shapes>>;
	andWhere(entityName: string, field: string | JsonRef<unknown>): unknown {
		if (this.#binaryOperator === "OR") {
			throw new Error(
				`Cannot convert ${this.#binaryOperator} where builder into 'AND WHERE'`,
			);
		}

		this.#binaryOperator = "AND";
		return this.#openComparator(entityName, field) as unknown;
	}

	orWhere<
		Alias extends string & keyof Shapes,
		Key extends string & keyof Shapes[Alias],
	>(
		entityName: Alias,
		field: Key | JsonRef<Shapes[Alias]>,
	): WhereQueryComparators<Shapes[Alias][Key], AndWhereQueryBuilder<Shapes>>;
	orWhere<Alias extends string & keyof Shapes>(
		entityName: Alias,
		field: JsonRef<Shapes[Alias]>,
	): WhereQueryComparators<object, AndWhereQueryBuilder<Shapes>>;
	orWhere(entityName: string, field: string | JsonRef<unknown>): unknown {
		if (this.#binaryOperator === "AND") {
			throw new Error(
				`Cannot convert ${this.#binaryOperator} where builder into 'OR WHERE'`,
			);
		}
		this.#binaryOperator = "OR";
		return this.#openComparator(entityName, field) as unknown;
	}

	#getColumnName(alias: string, field: string | JsonRef<unknown>) {
		if (isJsonRef(field)) {
			return sql.asUnescaped(`"${alias}".${readJsonRef(field)}`);
		}
		return sql.asUnescaped(`"${alias}"."${field}"`);
	}

	#openBaseComparator(column: string) {
		const getColumnName = () => sql.asUnescaped(column);
		const comparators: WhereQueryComparators<
			PostgresValueType,
			AndWhereQueryBuilder<Shapes> & OrWhereQueryBuilder<Shapes>
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

	#openComparator<Alias extends string & keyof Shapes>(
		alias: Alias,
		field: string | JsonRef<unknown>,
	) {
		const entity = this.#knownEntities.get(alias);
		if (!entity) {
			throw new Error(
				`Unrecognized entity alias in where: ${alias} (expected one of: ${JSON.stringify(
					Array.from(this.#knownEntities.keys()),
				)})`,
			);
		}

		return this.#openBaseComparator(
			this.#getColumnName(alias, field).value,
		) as unknown as WhereQueryComparators<
			unknown,
			AndWhereQueryBuilder<Shapes> & OrWhereQueryBuilder<Shapes>
		>;
	}

	#appendQuery(query: PreparedQuery) {
		if (this.#queries.length === 0) {
			return new InternalWhereBuilder<Shapes>(this.#knownEntities, [
				...this.#queries,
				query,
			]);
		}
		return new InternalWhereBuilder<Shapes>(this.#knownEntities, [
			...this.#queries,
			sql.prefixQuery(this.#binaryOperator!, query),
		]);
	}

	getConditionQuery(): PreparedQuery {
		if (this.#queries.length === 1) {
			return this.#queries[0];
		}
		return sql.brackets(joinAllQueries(this.#queries));
	}

	getQuery(): PreparedQuery {
		return sql.prefixQuery(` WHERE `, joinAllQueries(this.#queries));
	}
}

export type AndWhereQueryBuilder<Shapes extends Record<string, object>> = Pick<
	InternalWhereBuilder<Shapes>,
	"andWhere" | "getQuery"
>;
export type OrWhereQueryBuilder<Shapes extends Record<string, object>> = Pick<
	InternalWhereBuilder<Shapes>,
	"orWhere" | "getQuery"
>;
export type WhereQueryBuilder<Shapes extends Record<string, object>> =
	ReturnType<InternalWhereBuilder<Shapes>["getBuilder"]>;

export function createWhereBuilder<Shapes extends Record<string, object>>(
	knownEntities:
		| Map<
				string & keyof Shapes,
				Pick<EntityFromShape<unknown>, "schema" | "tableName">
		  >
		| Record<
				string & keyof Shapes,
				Pick<EntityFromShape<unknown>, "schema" | "tableName">
		  >,
): WhereQueryBuilder<Shapes> {
	const builder = new InternalWhereBuilder<Shapes>(
		knownEntities instanceof Map
			? knownEntities
			: new Map(Object.entries(knownEntities)),
	);
	return builder.getBuilder();
}

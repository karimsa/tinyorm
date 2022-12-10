import { EntityFromShape } from "./entity";
import {
	PreparedQuery,
	PostgresValueType,
	sql,
	joinAllQueries,
	finalizeQuery,
} from "./queries";

export type WhereQueryComparators<
	T extends PostgresValueType,
	NextQueryBuilder,
> = {
	Equals(value: T): NextQueryBuilder;
	NotEquals(value: T): NextQueryBuilder;
	EqualsAny(values: T[]): NextQueryBuilder;
	EqualsNone(values: T[]): NextQueryBuilder;
	NotLike(values: string): NextQueryBuilder;
	Like(values: string): NextQueryBuilder;
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
		const openWhere = <Alias extends string & keyof Shapes>(
			entityName: Alias,
			field: string & keyof Shapes[Alias],
		) => {
			if (this.#binaryOperator || this.#queries.length > 0) {
				throw new Error("Cannot re-open where builder");
			}
			return this.#openComparator(entityName, field);
		};

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
	>(entityName: Alias, field: Key) {
		if (this.#binaryOperator === "OR") {
			throw new Error(
				`Cannot convert ${this.#binaryOperator} where builder into 'AND WHERE'`,
			);
		}

		this.#binaryOperator = "AND";
		return this.#openComparator(
			entityName,
			field,
		) as unknown as WhereQueryComparators<
			Shapes[Alias][Key] & PostgresValueType,
			AndWhereQueryBuilder<Shapes>
		>;
	}

	orWhere<
		Alias extends string & keyof Shapes,
		Key extends string & keyof Shapes[Alias],
	>(entityName: Alias, field: Key) {
		if (this.#binaryOperator === "AND") {
			throw new Error(
				`Cannot convert ${this.#binaryOperator} where builder into 'OR WHERE'`,
			);
		}
		this.#binaryOperator = "OR";
		return this.#openComparator(
			entityName,
			field,
		) as unknown as WhereQueryComparators<
			Shapes[Alias][Key] & PostgresValueType,
			AndWhereQueryBuilder<Shapes>
		>;
	}

	#getColumnName(alias: string, field: string) {
		return sql.asUnescaped(`"${alias}"."${field}"`);
	}

	#openComparator<
		Alias extends string & keyof Shapes,
		Key extends string & keyof Shapes[Alias],
	>(alias: Alias, field: Key) {
		const entity = this.#knownEntities.get(alias);
		if (!entity) {
			throw new Error(
				`Unrecognized entity alias in where: ${alias} (expected one of: ${JSON.stringify(
					Array.from(this.#knownEntities.keys()),
				)})`,
			);
		}

		const comparators: Record<
			string,
			(value: PostgresValueType) => InternalWhereBuilder<Shapes>
		> = {
			Equals: (value) =>
				this.#appendQuery(sql`${this.#getColumnName(alias, field)} = ${value}`),
			NotEquals: (value) =>
				this.#appendQuery(
					sql`${this.#getColumnName(alias, field)} != ${value}`,
				),
			EqualsAny: (values) =>
				this.#appendQuery(
					sql`${this.#getColumnName(alias, field)} = ANY(${values})`,
				),
			EqualsNone: (values) =>
				this.#appendQuery(
					sql`${this.#getColumnName(alias, field)} <> ANY(${values})`,
				),
			Like: (value) =>
				this.#appendQuery(
					sql`${this.#getColumnName(alias, field)} LIKE ${value}`,
				),
			NotLike: (value) =>
				this.#appendQuery(
					sql`${this.#getColumnName(alias, field)} NOT LIKE ${value}`,
				),
		};
		return comparators as unknown as WhereQueryComparators<
			Shapes[Alias][Key] & PostgresValueType,
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

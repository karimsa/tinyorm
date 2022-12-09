import { EntityFromShape } from "./entity";
import {
	PreparedQuery,
	PostgresValueType,
	sql,
	PostgresSimpleValueType,
	joinAllQueries,
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

interface BaseWhereBuilder {
	getQuery(): PreparedQuery;
}

// TODO: Maybe restrict it to act as pure modifiers that only step forward
// otherwise the type safety is useless
export class InternalWhereBuilder<Shapes extends Record<string, object>>
	implements BaseWhereBuilder
{
	#binaryOperator: "AND" | "OR" | null = null;
	#queries: PreparedQuery[] = [];
	#knownEntities: Map<string, EntityFromShape<unknown>>;

	constructor(knownEntities: Map<string, EntityFromShape<unknown>>) {
		this.#knownEntities = knownEntities;
	}

	openWhere<Alias extends string & keyof Shapes>(
		entityName: Alias,
		field: string & keyof Shapes[Alias],
	) {
		if (this.#binaryOperator || this.#queries.length > 0) {
			throw new Error("Cannot re-open where builder");
		}
		return this.#openComparator(entityName, field);
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

	#getColumnName(entity: EntityFromShape<unknown>, field: string) {
		return sql.asUnescaped(`${entity.tableName}.${field}`);
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
			(value: PostgresValueType) => typeof this
		> = {
			Equals: (value) =>
				this.#appendQuery(
					sql`${this.#getColumnName(entity, field)} = ${value}`,
				),
			NotEquals: (value) =>
				this.#appendQuery(
					sql`${this.#getColumnName(entity, field)} != ${value}`,
				),
			EqualsAny: (values) =>
				this.#appendQuery(
					sql`${this.#getColumnName(entity, field)} = ANY(${values})`,
				),
			EqualsNone: (values) =>
				this.#appendQuery(
					sql`${this.#getColumnName(entity, field)} <> ANY(${values})`,
				),
			Like: (value) =>
				this.#appendQuery(
					sql`${this.#getColumnName(entity, field)} LIKE ${value}`,
				),
			NotLike: (value) =>
				this.#appendQuery(
					sql`${this.#getColumnName(entity, field)} NOT LIKE ${value}`,
				),
		};
		return comparators as unknown as WhereQueryComparators<
			Shapes[Alias][Key] & PostgresValueType,
			AndWhereQueryBuilder<Shapes> & OrWhereQueryBuilder<Shapes>
		>;
	}

	#appendQuery(query: PreparedQuery) {
		if (this.#queries.length === 0) {
			this.#queries.push(query);
		} else {
			this.#queries.push({
				...query,
				text: [
					`${this.#binaryOperator!} ${query.text[0]}`,
					...query.text.slice(1),
				],
			});
		}
		return this;
	}

	getQuery(): PreparedQuery {
		return joinAllQueries(this.#queries);
	}
}

export type AndWhereQueryBuilder<Shapes extends Record<string, object>> = Pick<
	InternalWhereBuilder<Shapes>,
	"andWhere"
>;
export type OrWhereQueryBuilder<Shapes extends Record<string, object>> = Pick<
	InternalWhereBuilder<Shapes>,
	"orWhere"
>;
export type WhereQueryBuilder<Shapes extends Record<string, object>> =
	InternalWhereBuilder<Shapes>["openWhere"];

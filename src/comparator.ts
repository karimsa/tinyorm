import {
	PreparedQuery,
	PostgresValueType,
	sql,
	PostgresSimpleValueType,
	joinAllQueries,
} from "./queries";

export interface WhereQueryComparators<
	T extends PostgresValueType,
	NextQueryBuilder,
> {
	Equals(value: T): NextQueryBuilder;
	NotEquals(value: T): NextQueryBuilder;
	EqualsAny(values: T[]): NextQueryBuilder;
	EqualsNone(values: T[]): NextQueryBuilder;
	NotLike(values: string): NextQueryBuilder;
	Like(values: string): NextQueryBuilder;
}

interface BaseWhereBuilder {
	getQuery(): PreparedQuery;
}

export interface AndWhereQueryBuilder<Shape extends object>
	extends BaseWhereBuilder {
	andWhere<Key extends string & keyof Shape>(
		key: Key,
	): WhereQueryComparators<
		Extract<Shape[Key], PostgresValueType>,
		AndWhereQueryBuilder<Shape>
	>;
}
export interface OrWhereQueryBuilder<Shape extends object>
	extends BaseWhereBuilder {
	orWhere<Key extends string & keyof Shape>(
		key: Key,
	): WhereQueryComparators<
		Extract<Shape[Key], PostgresValueType>,
		OrWhereQueryBuilder<Shape>
	>;
}

export interface WhereQueryBuilder<Shape extends object,> {
	<Key extends string & keyof Shape>(key: Key): WhereQueryComparators<
		Extract<Shape[Key], PostgresValueType>,
		AndWhereQueryBuilder<Shape> & OrWhereQueryBuilder<Shape>
	>;
}

// TODO: Maybe restrict it to act as pure modifiers that only step forward
// otherwise the type safety is useless
export class InternalWhereBuilder<Shape extends object,>
	implements BaseWhereBuilder
{
	#binaryOperator: "AND" | "OR" | null = null;
	#queries: PreparedQuery[] = [];
	readonly #entityName: string;

	constructor(entityName: string) {
		this.#entityName = entityName;
	}

	openWhere(field: string & keyof Shape) {
		if (this.#binaryOperator || this.#queries.length > 0) {
			throw new Error("Cannot re-open where builder");
		}
		return this.#openComparator(field);
	}

	andWhere(field: string & keyof Shape) {
		if (this.#binaryOperator === "OR") {
			throw new Error(
				`Cannot convert ${this.#binaryOperator} where builder into 'AND WHERE'`,
			);
		}
		this.#binaryOperator = "AND";
		return this.#openComparator(field);
	}

	orWhere(field: string & keyof Shape) {
		if (this.#binaryOperator === "AND") {
			throw new Error(
				`Cannot convert ${this.#binaryOperator} where builder into 'OR WHERE'`,
			);
		}
		this.#binaryOperator = "OR";
		return this.#openComparator(field);
	}

	#getColumnName(field: string) {
		return sql.asUnescaped(`${this.#entityName}.${field}`);
	}

	#openComparator(
		field: string & keyof Shape,
	): WhereQueryComparators<
		PostgresSimpleValueType,
		AndWhereQueryBuilder<Shape> & OrWhereQueryBuilder<Shape>
	> {
		return {
			Equals: (value) => {
				this.appendQuery(sql`${this.#getColumnName(field)} = ${value}`);
				// rome-ignore lint/suspicious/noExplicitAny: <explanation>
				return this as any;
			},
			NotEquals: (value) => {
				this.appendQuery(sql`${this.#getColumnName(field)} != ${value}`);
				// rome-ignore lint/suspicious/noExplicitAny: <explanation>
				return this as any;
			},
			EqualsAny: (values) => {
				this.appendQuery(sql`${this.#getColumnName(field)} = ANY(${values})`);
				// rome-ignore lint/suspicious/noExplicitAny: <explanation>
				return this as any;
			},
			EqualsNone: (values) => {
				this.appendQuery(sql`${this.#getColumnName(field)} <> ANY(${values})`);
				// rome-ignore lint/suspicious/noExplicitAny: <explanation>
				return this as any;
			},
			Like: (value) => {
				this.appendQuery(sql`${this.#getColumnName(field)} LIKE ${value}`);
				// rome-ignore lint/suspicious/noExplicitAny: <explanation>
				return this as any;
			},
			NotLike: (value) => {
				this.appendQuery(sql`${this.#getColumnName(field)} NOT LIKE ${value}`);
				// rome-ignore lint/suspicious/noExplicitAny: <explanation>
				return this as any;
			},
		};
	}

	private appendQuery(query: PreparedQuery) {
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
	}

	getQuery(): PreparedQuery {
		return joinAllQueries(this.#queries);
	}
}

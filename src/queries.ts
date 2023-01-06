import * as util from "util";
import { EntityFromShape, isEntity, PostgresColumnType } from "./entity";

/**
 * Defines non-array and non-null JS types that are allowed to be passed as query parameters.
 */
export type PostgresSimpleValueType = string | number | boolean | Date | object;

/**
 * Defines JS types that are allowed to be passed as query parameters.
 */
export type PostgresValueType =
	| PostgresSimpleValueType
	| (PostgresSimpleValueType | null)[]
	| null;

export const PostgresBooleanColumnTypes = ["bool", "boolean"] as const;
export type PostgresBooleanColumnType =
	typeof PostgresBooleanColumnTypes[number];

export const PostgresNumericColumnTypes = [
	"int",
	"int2",
	"int4",
	"int8",
	"smallint",
	"integer",
	"bigint",
	"decimal",
	"numeric",
	"real",
	"float",
	"float4",
	"float8",
	"double precision",
] as const;
export type PostgresNumericColumnType =
	typeof PostgresNumericColumnTypes[number];

export const PostgresStringColumnTypes = [
	"uuid",
	"character varying",
	"varchar",
	"character",
	"char",
	"text",
	"citext",
	"enum",
] as const;
export type PostgresStringColumnType = typeof PostgresStringColumnTypes[number];

export const PostgresDateColumnTypes = [
	"timetz",
	"timestamptz",
	"timestamp",
	"timestamp without time zone",
	"timestamp with time zone",
	"date",
	"time",
	"time without time zone",
	"time with time zone",
] as const;
export type PostgresDateColumnType = typeof PostgresDateColumnTypes[number];

export const PostgresJsonColumnTypes = ["json", "jsonb"] as const;
export type PostgresJsonColumnType = typeof PostgresJsonColumnTypes[number];

export interface PreparedQuery {
	text: string[];
	params: (QueryVariable | UnescapedVariable)[];
}

export interface FinalizedQuery {
	text: string;
	values: PostgresValueType[];
}

const kUnescapedVariable = Symbol("unescapedVariable");

export interface UnescapedVariable {
	type: typeof kUnescapedVariable;
	value: string;
}

type QueryVariable =
	| {
			type: string;
			isArray: false;
			value: PostgresSimpleValueType | null;
	  }
	| {
			type: string;
			isArray: true;
			value: (PostgresSimpleValueType | null)[];
	  };

// rome-ignore lint/suspicious/noExplicitAny: This is a type-guard.
function isQueryVariable(variable: any): variable is QueryVariable {
	return (
		typeof variable === "object" &&
		variable !== null &&
		(typeof variable.type === "string" ||
			variable.type === null ||
			variable.type === kUnescapedVariable) &&
		{}.hasOwnProperty.call(variable, "value")
	);
}

// rome-ignore lint/suspicious/noExplicitAny: This is a type-guard.
function isUnescapedVariable(variable: any): variable is UnescapedVariable {
	return (
		typeof variable === "object" &&
		variable !== null &&
		variable.type === kUnescapedVariable &&
		{}.hasOwnProperty.call(variable, "value")
	);
}

/**
 * This error is thrown when a query parameter is passed that tinyorm doesn't know how to handle.
 *
 * If you hit this error, you can always use a typecast helper to explicitly tell tinyorm what type the value is.
 */
export class UnknownQueryParameterTypeError extends Error {
	constructor(message: string, private readonly value: unknown) {
		super(message);
	}
}

function getPgTypeOf(
	value: PostgresValueType,
): Pick<QueryVariable, "isArray" | "type"> {
	if (Array.isArray(value)) {
		const firstValue = value.find((v) => v !== null);
		if (firstValue === undefined) {
			throw new UnknownQueryParameterTypeError(
				`Failed to find type for array: ${JSON.stringify(
					value,
				)} (use a typecast helper)`,
				value,
			);
		}

		const typeGuess = getPgTypeOf(firstValue);
		if (!typeGuess) {
			throw new UnknownQueryParameterTypeError(
				`Failed to find type for array: ${JSON.stringify(
					value,
				)} (use a typecast helper)`,
				value,
			);
		}

		return { ...typeGuess, isArray: true };
	}

	switch (typeof value) {
		case "string":
			return { type: "text", isArray: false };
		case "number":
			return { type: "double precision", isArray: false };
		case "boolean":
			return { type: "boolean", isArray: false };
	}

	if (value instanceof Date) {
		return { type: "timestamp", isArray: false };
	}

	if (value == null) {
		return { type: "null", isArray: false };
	}

	if (typeof value === "object") {
		return { type: "jsonb", isArray: false };
	}

	throw new UnknownQueryParameterTypeError(
		`Failed to find type for value: ${value} (use a typecast helper)`,
		value,
	);
}

function getPgValueOf({ type, value }: QueryVariable & { isArray: false }) {
	if (value == null) {
		return null;
	}

	switch (type) {
		case "integer":
		case "double precision":
			return Number(value);
		case "boolean":
			return Boolean(value);
		case "text":
		case "varchar":
			return String(value);
		case "jsonb":
			// This is going to come back and haunt me one day
			return typeof value === "string" ? value : JSON.stringify(value);
		case "timestamp":
			if (
				value &&
				(typeof value === "string" ||
					typeof value === "number" ||
					value instanceof Date)
			) {
				return new Date(value).toISOString();
			}
			throw Object.assign(
				new Error(`Invalid date value received: '${value}'`),
				{
					value,
				},
			);
		case "date":
			if (
				value &&
				(typeof value === "string" ||
					typeof value === "number" ||
					value instanceof Date)
			) {
				const date = new Date(value);
				return `${date.getFullYear()}-${leftPaddedInt(
					date.getMonth() + 1,
				)}-${leftPaddedInt(date.getDate())}`;
			}
			throw Object.assign(
				new Error(`Invalid date value received: '${value}'`),
				{
					value,
				},
			);

		default:
			throw new Error(`Unsupported postgres type: '${String(type)}'`);
	}
}

export function getQueryVariable(
	variable: PostgresValueType | QueryVariable,
	typeHint?: QueryVariable["type"],
): QueryVariable {
	if (isQueryVariable(variable)) {
		return variable;
	}

	const pgType = typeHint
		? { type: typeHint, isArray: false }
		: getPgTypeOf(variable);
	if (pgType.isArray) {
		if (!Array.isArray(variable)) {
			throw new Error("Unexpected state reached");
		}
		return {
			type: pgType.type,
			isArray: true,
			value: variable.map((v) =>
				v == null
					? null
					: getPgValueOf({ type: pgType.type, value: v, isArray: false }),
			),
		};
	}

	if (Array.isArray(variable)) {
		throw new Error("Unexpected state reached");
	}
	return {
		type: pgType.type,
		isArray: false,
		value: getPgValueOf({ type: pgType.type, value: variable, isArray: false }),
	};
}

function leftPaddedInt(num: number) {
	return num < 10 ? `0${num}` : `${num}`;
}

export function castValue(
	name: string,
	queryVar: Exclude<QueryVariable, UnescapedVariable>,
) {
	const typeCast = queryVar.type === "null" ? "" : `::${queryVar.type}`;
	return `${name}${typeCast}`;
}

const kJsonRef = Symbol("jsonRef");
const kJsonEntityRef = Symbol("jsonEntityRef");

export type JsonRef<Shape> = Record<typeof kJsonRef, string> & {
	[kJsonEntityRef]: Shape;
};

const createJsonRefProxy = (jsonRef: {
	column: string;
	jsonPath: string[];
}) =>
	new Proxy(
		{},
		{
			get: (_, key): unknown =>
				typeof key === "string"
					? createJsonRefProxy({
							column: jsonRef.column || key,
							jsonPath: jsonRef.column
								? [...jsonRef.jsonPath, key]
								: jsonRef.jsonPath,
					  })
					: key === kJsonRef
					? `"${jsonRef.column}"${jsonRef.jsonPath
							.map((key) =>
								// This is a bit hacky but it's the only way to check if a string is a number, since
								// in JS, array indices are strings as well.
								isNaN(Number(String(key))) ? `->"${key}"` : `->${key}`,
							)
							.join("")}`
					: null,
		},
	);

type JsonRefBuilder<EntityShape, Shape> = JsonRef<EntityShape> &
	(Shape extends (infer ElmType)[]
		? {
				[Key: number]: JsonRefBuilder<EntityShape, ElmType>;
		  }
		: Shape extends string
		? {}
		: {
				[Key in keyof Shape]: JsonRefBuilder<EntityShape, Shape[Key]>;
		  });

export function readJsonRef(value: JsonRef<unknown>): string {
	return value[kJsonRef];
}

// rome-ignore lint/suspicious/noExplicitAny: This is a type-guard.
export function isJsonRef(value: any): value is JsonRef<unknown> {
	return (
		typeof value === "object" &&
		value !== null &&
		// This requires a double-eval atm, since the proxy does
		// not respond to 'has' and this checks the underlying type
		typeof value[kJsonRef] === "string"
	);
}

export interface SqlHelpers {
	//
	// Casting helpers
	//

	/**
	 * Marks a value as unescaped. Only use this with values that are trusted, because
	 * it wil not be marked as a query paramter.
	 *
	 * Please note that unescaped values are treated as raw SQL, and not as a value. This
	 * means they _must_ be a string, and will not be stringified for you.
	 *
	 * ```ts
	 * enum MyEnum {
	 * 	Foo = 'foo',
	 * 	Bar = 'bar',
	 * }
	 *
	 * // Enum values are usually safe to unescape, since they are not user input
	 * sql`SELECT * FROM user where role = '${sql.asUnescaped(MyEnum.Foo)}'`
	 * ```
	 *
	 * @param value
	 * @returns
	 */
	asUnescaped(value: string): UnescapedVariable;

	/**
	 * Marks a query parameter as a 'TEXT' type in postgres. This generates something like `$1::text`.
	 *
	 * ```ts
	 * // Generates: SELECT * FROM user where name = $1::text
	 * sql`SELECT * FROM user where name = ${sql.asText("foo")}`
	 * ```
	 *
	 * @param value value that should be treated as a string
	 */
	asText(value: string | number | boolean): QueryVariable;

	/**
	 * Marks a query parameter as a 'BOOLEAN' type in postgres. This generates something like `$1::boolean`.
	 *
	 * ```ts
	 * // Generates: SELECT * FROM user where active = $1::boolean
	 * sql`SELECT * FROM user where active = ${sql.asBool(true)}`
	 * ```
	 *
	 * @param value value that should be treated as a boolean
	 */
	asBool(value: unknown): QueryVariable;

	/**
	 * Marks a query parameter as a 'date'. This generates something like `$1::date`, where the query parameter
	 * passed to postgres is an ISO string (Postgres should forcefully truncate this to a date).
	 *
	 * Please handle the timezone yourself.
	 *
	 * ```ts
	 * // Generates: SELECT * FROM user where created_at = $1::date
	 * sql`SELECT * FROM user where created_at = ${sql.asDate(new Date())}`
	 * ```
	 *
	 * @param date a valid JS date object
	 */
	asDate(date: Date): QueryVariable;

	/**
	 * Marks a query parameter as a 'timestamp without time zone'. This generates something like `$1::timestamp without time zone`.
	 *
	 * ```ts
	 * // Generates: SELECT * FROM user where created_at = $1::timestamp without time zone
	 * sql`SELECT * FROM user where created_at = ${sql.asTimestamp(new Date())}`
	 * ```
	 *
	 * @param date
	 */
	asTimestamp(date: Date): QueryVariable;

	/**
	 * Marks a query parameter as a 'jsonb'. This generates something like `$1::jsonb`.
	 * If an object is passed, it will be stringified using `JSON.stringify`. If you want to
	 * handle serialization yourself, you can also pass a string.
	 *
	 * ```ts
	 * // Generates: SELECT * FROM user where data = $1::jsonb
	 * sql`SELECT * FROM user where data = ${sql.asJSONB({ foo: "bar" )}`
	 * ```
	 *
	 * @param value a JSON object or string
	 */
	asJSONB(value: string | object): QueryVariable;

	/**
	 * Marks a value as a custom postgres type.
	 * @param value any serializable postgres value
	 * @param type casted value type
	 */
	asCastedValue(
		value:
			| Exclude<PostgresSimpleValueType, Date | object>
			| Exclude<PostgresSimpleValueType, Date | object>[]
			| null,
		type: PostgresColumnType | `${PostgresColumnType}[]`,
	): QueryVariable;

	/**
	 * Builds an entity reference for aliased entities. This is useful for joins.
	 *
	 * ```ts
	 * class User extends Entity({ schema: 'app', tableName: 'user' }) {}
	 * class Post extends Entity({ schema: 'app', tableName: 'post' }) {}
	 *
	 * // Generates: SELECT * FROM "app"."user" AS "u" INNER JOIN "app"."post" AS "p" ON u.id = p.user_id
	 * sql`SELECT * FROM ${sql.getEntityRef(User, "u")} INNER JOIN ${sql.getEntityRef(Post, "p")} ON u.id = p.user_id`
	 * ```
	 *
	 * Note: If you want to refer to an entity without an alias, the entity can be passed directly in your query.
	 *
	 * ```ts
	 * // Generates: SELECT * FROM "app"."user"
	 * sql`SELECT * FROM ${User}`
	 * ```
	 *
	 * @param entity the tinyorm entity to reference
	 */
	getEntityRef(
		entity: Pick<EntityFromShape<unknown>, "schema" | "tableName">,
		alias?: string,
	): UnescapedVariable;

	/**
	 * Helper that allows you to create JSON path references in a type-safe way.
	 *
	 * ```ts
	 * class User extends Entity({ tableName: 'user' }) {
	 *   readonly id: string;
	 *   readonly data: { foo: { bar: string }[] };
	 * }
	 *
	 * // Generates: SELECT * FROM "user" WHERE "data"->'foo'->0->'bar' = 'baz'
	 * sql`SELECT * FROM ${User} WHERE (${sql.json(User).data.foo[0].bar})::text = 'baz'`;
	 * ```
	 *
	 * @param entity the tinyorm entity to reference
	 */
	json<Shape>(_: EntityFromShape<Shape>): JsonRefBuilder<Shape, Shape>;

	/**
	 * Joins a set of PreparedQueries together into a single PreparedQuery.
	 * @param queries
	 * @returns joined query
	 */
	join(queries: PreparedQuery[], delim?: PreparedQuery): PreparedQuery;

	/**
	 * Creates a PreparedQuery that completely escapes query parameters. Only use this
	 * with trusted input, or have fun with your SQL injection.
	 *
	 * Example:
	 *
	 * ```ts
	 * // This will run: SELECT * FROM users;
	 * sql.unescaped(`SELECT * FROM ${'users'}`)
	 *
	 * // This will run: SELECT * FROM $1::text; with params: ['users']
	 * sql(`SELECT * FROM ${'users'}`)
	 * ```
	 *
	 * @param text
	 * @returns
	 */
	unescaped(text: string): PreparedQuery;

	/**
	 * Adds given prefix to the query.
	 */
	prefixQuery(prefix: string, query: PreparedQuery): PreparedQuery;

	/**
	 * Adds given suffix to the query.
	 */
	suffixQuery(query: PreparedQuery, suffix: string): PreparedQuery;

	/**
	 * Wraps a query with a prefix + suffix.
	 */
	wrapQuery(
		prefix: string,
		query: PreparedQuery,
		suffix: string,
	): PreparedQuery;

	/**
	 * Wraps a query with round brackets (or parantheses, if you're American).
	 *
	 * ```ts
	 * // Generates: (SELECT * FROM users)
	 * sql.brackets(sql`SELECT * FROM users`)
	 * ```
	 */
	brackets(query: PreparedQuery): PreparedQuery;

	/**
	 * Finalizes a PreparedQuery into a FinalizedQuery.
	 *
	 * PreparedQueries are raw queries that cannot be executed against postgres yet,
	 * because not all the parameters are known yet.
	 *
	 * FinalizedQueries are queries that can be executed against postgres, because
	 * all the parameters are known, so placeholders can be generated correctly.
	 *
	 * A FinalizedQuery cannot be modified, but PreparedQueries can be modified.
	 */
	finalize(query: PreparedQuery): FinalizedQuery;
}

const sqlHelpers: SqlHelpers = {
	asUnescaped: (value) => {
		return {
			type: kUnescapedVariable,
			value,
		};
	},

	asText: (value) => {
		return getQueryVariable(value, "text");
	},

	asBool(value) {
		return getQueryVariable(!!value, "boolean");
	},

	asDate: (date) => {
		return getQueryVariable(date, "date");
	},

	asTimestamp: (date) => {
		return getQueryVariable(date, "timestamp");
	},

	asJSONB: (value) => {
		if (typeof value === "object") {
			return getQueryVariable(JSON.stringify(value), "jsonb");
		}
		return getQueryVariable(value, "jsonb");
	},

	asCastedValue: (value, type) =>
		({
			type,
			isArray: type.endsWith("[]"),
			value,
		}) as unknown as QueryVariable,

	getEntityRef: (entity, alias) => {
		if (alias) {
			return sql.asUnescaped(
				`"${entity.schema}"."${entity.tableName}" AS "${alias}"`,
			);
		}
		return sql.asUnescaped(`"${entity.schema}"."${entity.tableName}"`);
	},

	json<Shape>(_: EntityFromShape<Shape>) {
		return createJsonRefProxy({
			column: "",
			jsonPath: [],
		}) as unknown as JsonRefBuilder<Shape, Shape>;
	},

	join: (queries, delim) => {
		if (queries.length === 0) {
			throw new Error("Cannot join zero queries");
		}
		if (delim) {
			return sql.join(
				queries.flatMap((query, index) =>
					index > 0 ? [delim, query] : [query],
				),
			);
		}

		return sql(["", ...queries.map(() => "")], ...queries);
	},

	unescaped: (text) => {
		return {
			text: [text],
			params: [],
		};
	},

	prefixQuery: (prefix, query) => {
		const text = [...query.text];
		text[0] = `${prefix}${query.text[0]}`;

		return {
			text,
			params: [...query.params],
		};
	},

	suffixQuery: (query, suffix) => {
		const text = [...query.text];
		text[query.text.length - 1] = `${
			query.text[query.text.length - 1]
		}${suffix}`;

		return {
			text,
			params: [...query.params],
		};
	},

	wrapQuery: (prefix, query, suffix) => {
		return sqlHelpers.suffixQuery(
			sqlHelpers.prefixQuery(prefix, query),
			suffix,
		);
	},

	brackets: (query) => {
		return sqlHelpers.wrapQuery("(", query, ")");
	},

	finalize: (query) => {
		const finalizedQuery: FinalizedQuery = {
			text: query.text[0] ?? "",
			values: [],
		};
		for (const [index, queryVar] of query.params.entries()) {
			if (isUnescapedVariable(queryVar)) {
				finalizedQuery.text += `${queryVar.value}${query.text[index + 1]}`;
			} else {
				finalizedQuery.text += `${castValue(
					`$${finalizedQuery.values.length + 1}`,
					queryVar,
				)} ${query.text[index + 1]}`;
				finalizedQuery.values.push(queryVar.value);
			}
		}
		return finalizedQuery;
	},
};

export type QueryParameterType =
	| EntityFromShape<unknown>
	| PostgresValueType
	| PreparedQuery
	| QueryVariable
	| UnescapedVariable
	| undefined;

/**
 * Helper that allows you to write SQL prepared queries as template strings. All template string
 * parameters are automatically escaped as SQL parameters. This function also contains a set of
 * useful utilities that allow you to perform type-casting and other operations.
 *
 * ```ts
 * // 'name' will be treated as user input and escaped
 * // This generates: `SELECT * FROM user WHERE name = $1::text`
 * sql`SELECT * FROM user WHERE name = ${name}`
 * ```
 *
 * @returns a PreparedQuery object
 *
 * @docs
 *
 * {@embedDocs SqlHelpers}
 */
export const sql = Object.assign(
	(
		templateStrings: ReadonlyArray<string>,
		...parameters: QueryParameterType[]
	): PreparedQuery => {
		const preparedQuery: PreparedQuery = {
			text: [templateStrings[0]],
			params: [],
		};

		for (const [index, param] of parameters.entries()) {
			if (isUnescapedVariable(param)) {
				preparedQuery.text.push(templateStrings[index + 1]);
				preparedQuery.params.push(param);
			} else if (isEntity(param)) {
				preparedQuery.text.push(templateStrings[index + 1]);
				preparedQuery.params.push(sql.getEntityRef(param));
			} else if (isJsonRef(param)) {
				preparedQuery.text.push(templateStrings[index + 1]);
				preparedQuery.params.push(sql.asUnescaped(param[kJsonRef]));
			} else if (isPreparedQuery(param)) {
				const lastText = preparedQuery.text.pop() ?? "";
				const mergedParam = sql.wrapQuery(
					lastText,
					param,
					templateStrings[index + 1],
				);

				preparedQuery.text.push(...mergedParam.text);
				preparedQuery.params.push(...mergedParam.params);
			} else if (isFinalizedQuery(param)) {
				throw new Error(
					`Cannot inline a finalized query into a prepared query`,
				);
			} else {
				const queryVar = getQueryVariable(param ?? null);

				preparedQuery.text.push(templateStrings[index + 1]);
				preparedQuery.params.push(queryVar);
			}
		}

		return {
			...preparedQuery,
			text: preparedQuery.text,
		};
	},
	sqlHelpers,
);

export function isFinalizedQuery(query: unknown): query is FinalizedQuery {
	return (
		typeof query === "object" &&
		query !== null &&
		typeof (query as FinalizedQuery).text === "string" &&
		Array.isArray((query as FinalizedQuery).values)
	);
}

export function isPreparedQuery(query: unknown): query is PreparedQuery {
	return (
		typeof query === "object" &&
		query !== null &&
		Array.isArray((query as PreparedQuery).text) &&
		Array.isArray((query as PreparedQuery).params)
	);
}

export function serializePostgresValue(value: unknown) {
	if (
		value === null ||
		typeof value === "boolean" ||
		typeof value === "number" ||
		typeof value === "string"
	) {
		return value;
	}
	if (value instanceof Date) {
		return value.toISOString();
	}
	if (typeof value === "object") {
		return JSON.stringify(value);
	}
	throw new Error(
		`Cannot serialize value: ${util.inspect(
			value,
		)} (use sql.asCastedValue or sql.asUnescaped))`,
	);
}

function serializePostgresValueAsString(value: unknown) {
	if (value === null) {
		return "null";
	}

	const serialized = serializePostgresValue(value);
	if (typeof serialized === "string") {
		return `'${serialized}'`;
	}
	return String(serialized);
}

export function unsafeFlattenQuery(query: PreparedQuery): PreparedQuery {
	const finalizedQuery = sql.finalize({
		text: query.text,
		params: query.params.map((param) => {
			if (isUnescapedVariable(param)) {
				return param;
			}
			return sql.asUnescaped(serializePostgresValueAsString(param.value));
		}),
	});
	return {
		text: [finalizedQuery.text],
		params: [],
	};
}

import { EntityFromShape, isEntity } from "./entity";

export type PostgresSimpleValueType = string | number | boolean | Date | object;

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

type UnescapedVariable = {
	type: typeof kUnescapedVariable;
	value: string;
};

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
	const arraySuffix = queryVar.isArray ? "[]" : "";
	return `${name}${typeCast}${arraySuffix}`;
}

const typeCastHelpers = {
	// Casts
	asUnescaped: (value: string): UnescapedVariable => ({
		type: kUnescapedVariable,
		value,
	}),
	asText: (value: string | number | boolean) => getQueryVariable(value, "text"),
	asBool: (value: unknown) => getQueryVariable(!!value, "boolean"),
	asDate: (date: Date): QueryVariable => getQueryVariable(date, "date"),
	asTimestamp: (date: Date): QueryVariable =>
		getQueryVariable(date, "timestamp"),
	asJSONB: (value: string) => getQueryVariable(value, "jsonb"),
};

const getEntityRef = (
	entity: Pick<EntityFromShape<unknown>, "schema" | "tableName">,
	alias?: string,
) => {
	if (alias) {
		return sql.asUnescaped(
			`"${entity.schema}"."${entity.tableName}" AS "${alias}"`,
		);
	}
	return sql.asUnescaped(`"${entity.schema}"."${entity.tableName}"`);
};

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

export const sql = Object.assign(
	(
		templateStrings: ReadonlyArray<string>,
		...parameters: (
			| EntityFromShape<unknown>
			| PostgresValueType
			| PreparedQuery
			| QueryVariable
			| UnescapedVariable
			| undefined
		)[]
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
	{
		...typeCastHelpers,
		getEntityRef,

		json<Shape>(_: EntityFromShape<Shape>) {
			return createJsonRefProxy({
				column: "",
				jsonPath: [],
			}) as unknown as JsonRefBuilder<Shape, Shape>;
		},

		join(queries: PreparedQuery[]): PreparedQuery {
			if (queries.length === 0) {
				throw new Error("Cannot join zero queries");
			}

			return sql(["", ...queries.map(() => "")], ...queries);
		},

		unescaped: (text: string): PreparedQuery => ({
			text: [text],
			params: [],
		}),

		/**
		 * Adds given prefix to the query.
		 */
		prefixQuery(prefix: string, query: PreparedQuery) {
			const text = [...query.text];
			text[0] = `${prefix}${query.text[0]}`;

			return {
				text,
				params: [...query.params],
			};
		},

		/**
		 * Adds given prefix to the query.
		 */
		suffixQuery(query: PreparedQuery, suffix: string) {
			const text = [...query.text];
			text[query.text.length - 1] = `${
				query.text[query.text.length - 1]
			}${suffix}`;

			return {
				text,
				params: [...query.params],
			};
		},

		/**
		 * Wraps a query with a prefix + suffix.
		 */
		wrapQuery(prefix: string, query: PreparedQuery, suffix: string) {
			return this.suffixQuery(this.prefixQuery(prefix, query), suffix);
		},

		/**
		 * Wraps a query with a prefix + suffix.
		 */
		brackets(query: PreparedQuery) {
			return this.wrapQuery("(", query, ")");
		},
	},
);

// Finalizes a prepared query into a query that is accepted by 'pg'
export function finalizeQuery(query: PreparedQuery): FinalizedQuery {
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
}

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

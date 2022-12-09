import { EntityFromShape } from "./entity";

export type PostgresSimpleValueType = string | number | boolean | Date;

export type PostgresValueType =
	| PostgresSimpleValueType
	| (PostgresSimpleValueType | null)[]
	| null;

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

// rome-ignore lint/suspicious/noExplicitAny: <explanation>
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

// rome-ignore lint/suspicious/noExplicitAny: <explanation>
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
			console.dir({ value, firstValue });
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
};

const getEntityRef = (
	entity: Pick<EntityFromShape<unknown>, "schema" | "tableName">,
	alias?: string,
) => {
	if (alias) {
		return sql.asUnescaped(`${entity.schema}.${entity.tableName} AS ${alias}`);
	}
	return sql.asUnescaped(`${entity.schema}.${entity.tableName}`);
};

export const sql = Object.assign(
	(
		templateStrings: TemplateStringsArray,
		...parameters: (
			| PostgresValueType
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
			finalizedQuery.text += `${queryVar.value} ${query.text[index + 1]}`;
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

export function joinQueries(
	left: PreparedQuery,
	right: PreparedQuery,
): PreparedQuery {
	if (left.text.length === 0) {
		return right;
	}
	if (right.text.length === 0) {
		return left;
	}

	return {
		text: [
			...left.text.slice(0, left.text.length - 1),
			`${left.text[left.text.length - 1]}${right.text[0]}`,
			...right.text.slice(1),
		],
		params: [...left.params, ...right.params],
	};
}

export function joinAllQueries(queries: PreparedQuery[]): PreparedQuery {
	let joinedQuery = queries[0];
	if (!joinedQuery) {
		throw new Error("Cannot join zero queries");
	}

	for (const [index, query] of queries.entries()) {
		if (index > 0) {
			joinedQuery = joinQueries(joinedQuery, query);
		}
	}

	return joinedQuery;
}

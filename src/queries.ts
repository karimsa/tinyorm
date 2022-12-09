type PostgresValueType = string | number | boolean | Date | null | undefined;

export interface PreparedQuery {
	text: string[];
	params: QueryVariable[];
}

export interface FinalizedQuery {
	text: string;
	values: PostgresValueType[];
}

interface QueryVariable {
	type: string | null;
	value: PostgresValueType;
}

// rome-ignore lint/suspicious/noExplicitAny: <explanation>
function isQueryVariable(variable: any): variable is QueryVariable {
	return (
		typeof variable === "object" &&
		variable !== null &&
		typeof variable.type === "string" &&
		{}.hasOwnProperty.call(variable, "value")
	);
}

function getPgTypeOf(value: PostgresValueType) {
	switch (typeof value) {
		case "string":
			return "text";
		case "number":
			return "double precision";
		case "boolean":
			return "boolean";
	}

	if (value instanceof Date) {
		return "timestamp";
	}

	if (value == null) {
		return null;
	}

	throw new Error(
		`Failed to find type for value: ${value} (use a typecast helper)`,
	);
}

function getPgValueOf(type: string | null, value: unknown) {
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
		case null:
			return null;

		default:
			throw new Error(`Unsupported postgres type: '${type}'`);
	}
}

function getQueryVariable(
	variable: PostgresValueType | QueryVariable,
	typeHint?: string,
): QueryVariable {
	if (isQueryVariable(variable)) {
		return variable;
	}

	const pgType = typeHint ?? getPgTypeOf(variable);
	return {
		type: pgType,
		value: getPgValueOf(pgType, variable),
	};
}

function leftPaddedInt(num: number) {
	return num < 10 ? `0${num}` : `${num}`;
}

export const sql = Object.assign(
	(
		templateStrings: TemplateStringsArray,
		...parameters: (PostgresValueType | QueryVariable)[]
	): PreparedQuery => {
		const preparedQuery: PreparedQuery = {
			text: [templateStrings[0]],
			params: [],
		};

		for (const [index, param] of parameters.entries()) {
			const queryVar = getQueryVariable(param);

			preparedQuery.text.push(templateStrings[index + 1]);
			preparedQuery.params.push(queryVar);
		}

		return {
			...preparedQuery,
			text: preparedQuery.text,
		};
	},
	{
		asText: (value: string | number | boolean) =>
			getQueryVariable(value, "text"),
		asBool: (value: unknown) => getQueryVariable(!!value, "boolean"),
		asDate: (date: Date): QueryVariable => getQueryVariable(date, "date"),
		asTimestamp: (date: Date): QueryVariable =>
			getQueryVariable(date, "timestamp"),
	},
);

// Finalizes a prepared query into a query that is accepted by 'pg'
export function finalizeQuery(query: PreparedQuery): FinalizedQuery {
	const finalizedQuery: FinalizedQuery = {
		text: query.text[0] ?? "",
		values: [],
	};
	for (const [index, queryVar] of query.params.entries()) {
		const typeCast = queryVar.type === null ? "" : `::${queryVar.type}`;

		finalizedQuery.text += `$${index + 1}${typeCast}${query.text[index + 1]}`;
		finalizedQuery.values.push(queryVar.value);
	}
	return finalizedQuery;
}

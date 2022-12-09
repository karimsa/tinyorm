import snakeCase from "lodash.snakecase";

export const assertCase = (name: string, value: string) => {
	if (snakeCase(value) !== value) {
		throw new Error(
			`Unexpected non-snakeCase ${name} in entity: '${value}' (expected: ${snakeCase(
				value,
			)})`,
		);
	}
};

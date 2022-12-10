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

export function assertType<T>(value: T) {}

export function isElementOfArray(elm: unknown, list: readonly unknown[]) {
	return list.includes(elm);
}

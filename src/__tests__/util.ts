import { FinalizedQuery } from "../queries";
import { format } from "sql-formatter";

const pretty = (text: string) => {
	try {
		return format(text, { language: "postgresql" });
	} catch (err) {
		throw new Error(`Failed to format: '${text}'`);
	}
};

export function expectQuery(given: FinalizedQuery) {
	return {
		toEqual(expected: FinalizedQuery) {
			expect(given).toBeDefined();
			expect({ text: pretty(given.text), values: given.values }).toMatchObject({
				text: pretty(expected.text),
				values: expected.values,
			});
		},
	};
}

// rome-ignore lint/suspicious/noExplicitAny: <explanation>
export function getResolvedType<T extends (...args: any) => any>(
	fn: T,
): Awaited<ReturnType<T>> {
	// rome-ignore lint/suspicious/noExplicitAny: <explanation>
	return null as any;
}

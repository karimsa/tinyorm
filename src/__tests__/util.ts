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
			expect(pretty(given.text)).toEqual(pretty(expected.text));
			expect(given).toMatchObject({
				text: given.text,
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

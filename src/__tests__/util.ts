import { FinalizedQuery } from "../queries";

export function expectQuery(given: FinalizedQuery) {
	return {
		toEqual(expected: FinalizedQuery) {
			expect(given.text.trim().replace(/\s+/g, " ")).toEqual(
				expected.text.trim().replace(/\s+/g, " "),
			);
			expect(given.values).toEqual(expected.values);
		},
	};
}

export function assertType<T>(value: T) {}

// rome-ignore lint/suspicious/noExplicitAny: <explanation>
export function getResolvedType<T extends (...args: any) => any>(
	fn: T,
): Awaited<ReturnType<T>> {
	// rome-ignore lint/suspicious/noExplicitAny: <explanation>
	return null as any;
}

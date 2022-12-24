import { format } from "sql-formatter";
import {
	FinalizedQuery,
	isPreparedQuery,
	PreparedQuery,
	sql,
} from "../queries";

const pretty = (text: string) => {
	try {
		return format(text, { language: "postgresql" });
	} catch (err) {
		throw new Error(`Failed to format: '${text}'`);
	}
};

export function expectQuery(given: FinalizedQuery | PreparedQuery) {
	return {
		toEqual(expected: FinalizedQuery | PreparedQuery) {
			const finalGiven = isPreparedQuery(given) ? sql.finalize(given) : given;
			const finalExpected = isPreparedQuery(expected)
				? sql.finalize(expected)
				: expected;

			expect(finalGiven).toBeDefined();
			expect({
				text: pretty(finalGiven.text),
				values: finalGiven.values,
			}).toMatchObject({
				text: pretty(finalExpected.text),
				values: finalExpected.values,
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

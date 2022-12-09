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

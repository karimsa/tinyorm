import { describe, it } from "@jest/globals";
import { Column, createInsertBuilder, Entity } from "../";
import { assertType } from "../utils";
import { expectQuery, getResolvedType } from "./util";

class TestUser extends Entity({ schema: "public", tableName: "test_user" }) {
	@Column({ type: 'uuid' })
	readonly id!: string;
	@Column({ type: 'text' })
	readonly name!: string;
	@Column({ type: 'jsonb' })
	readonly meta!: { isCool: boolean };
}

describe("InsertBuilder", () => {
	it("should allow inserting new rows", () => {
		expectQuery(
			createInsertBuilder(TestUser)
				.addRows([
					{
						id: "6f0dea07-dbf6-4e6b-9e3b-8df47d278628",
						name: "test",
						meta: { isCool: true },
					},
				])
				.getQuery(),
		).toEqual({
			text: `
                INSERT INTO "public"."test_user" ("id", "name", "meta")
                VALUES ($1::text, $2::text, $3::jsonb)
            `,
			values: [
				"6f0dea07-dbf6-4e6b-9e3b-8df47d278628",
				"test",
				`{"isCool":true}`,
			],
		});
	});
	it("should allow return selected columns during inserts", () => {
		const builder = createInsertBuilder(TestUser)
			.addRows([
				{
					id: "6f0dea07-dbf6-4e6b-9e3b-8df47d278628",
					name: "test",
					meta: { isCool: true },
				},
			])
			.returning(["id"]);

		assertType<{ id: string }[]>(getResolvedType(builder.execute));
		expectQuery(builder.getQuery()).toEqual({
			text: `
                INSERT INTO "public"."test_user" ("id", "name", "meta")
                VALUES ($1::text, $2::text, $3::jsonb)
				RETURNING ("id")
            `,
			values: [
				"6f0dea07-dbf6-4e6b-9e3b-8df47d278628",
				"test",
				`{"isCool":true}`,
			],
		});
	});
});

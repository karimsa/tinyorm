import { describe, it } from "@jest/globals";
import { createWhereBuilder, finalizeQuery } from "../";
import { expectQuery } from "./util";

describe("WhereBuilder", () => {
	it("should allow building where clauses", () => {
		const where = createWhereBuilder<{
			user: { id: string; name: string; status: "Active" | "Inactive" };
			userPost: { user_id: string; post_id: string; reactions: string[] };
			post: { id: string; author_id: string };
		}>({
			user: { schema: "app", tableName: "user" },
			userPost: { schema: "app", tableName: "user_post" },
			post: { schema: "app", tableName: "post" },
		});

		// Find all users with a name similar to 'Karim'
		expectQuery(
			finalizeQuery(where("user", "name").Like("%Karim%").getQuery()),
		).toEqual({
			text: `
                WHERE "user"."name" LIKE $1::text
            `,
			values: ["%Karim%"],
		});

		// Find all users with a name similar to 'Karim' OR is any of ('Bob', 'Alice')
		expectQuery(
			finalizeQuery(
				where("user", "name")
					.Like("%Karim%")
					.orWhere("user", "name")
					.EqualsAny(["Bob", "Alice"])
					.getQuery(),
			),
		).toEqual({
			text: `
                WHERE "user"."name" LIKE $1::text OR "user"."name" = ANY($2::text[])
            `,
			values: ["%Karim%", ["Bob", "Alice"]],
		});
		expectQuery(
			finalizeQuery(
				where
					.all([
						where.either([
							where("user", "name").Like("%Karim%"),
							where("user", "name").EqualsAny(["Bob", "Alice"]),
						]),
						where("user", "status").Equals("Active"),
					])
					.getQuery(),
			),
		).toEqual({
			text: `
                WHERE ("user"."name" LIKE $1::text OR "user"."name" = ANY($2::text[]))
				  AND "user"."status" = $3::text
            `,
			values: ["%Karim%", ["Bob", "Alice"], "Active"],
		});
	});
});

import { describe, it } from "@jest/globals";
import { createWhereBuilder, finalizeQuery, sql, Entity } from "../";
import { expectQuery } from "./util";

describe("WhereBuilder", () => {
	class User extends Entity({ schema: "public", tableName: "users" }) {
		id: string;
	}
	class Post extends Entity({ schema: "public", tableName: "posts" }) {
		id: string;
		author_id: string;
		content: {
			type: "text" | "image";
			value: string;
			nestedObject: { hello: string; isBool: boolean };
			nestedArray: string[];
		};
	}
	const where = createWhereBuilder<{
		user: { id: string; name: string; status: "Active" | "Inactive" };
		userPost: { user_id: string; post_id: string; reactions: string[] };
		post: Post;
	}>({
		user: { schema: "app", tableName: "user" },
		userPost: { schema: "app", tableName: "user_post" },
		post: { schema: "app", tableName: "post" },
	});

	it("should allow building where clauses", () => {
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

	it("should allow JSONB queries", () => {
		// Check top-level JSON field value
		expectQuery(
			finalizeQuery(
				where("post", "content")
					.JsonProperty("type")
					.CastAs("text")
					.Equals("text")
					.getQuery(),
			),
		).toEqual({
			text: `
                WHERE ("post"."content"->"type")::text = $1::text
            `,
			values: ["text"],
		});

		// Check nested JSON field value
		expectQuery(
			finalizeQuery(
				where("post", "content")
					.JsonPath("nestedObject.hello")
					.CastAs("text")
					.Equals("world")
					.getQuery(),
			),
		).toEqual({
			text: `
                WHERE ("post"."content"->"nestedObject"->"hello")::text = $1::text
            `,
			values: ["world"],
		});
		expectQuery(
			finalizeQuery(
				where("post", sql.json(Post).content.nestedObject.isBool)
					.CastAs("boolean")
					.Equals(true)
					.getQuery(),
			),
		).toEqual({
			text: `
                WHERE ("post"."content"->"nestedObject"->"isBool")::text::boolean = $1::boolean
            `,
			values: [true],
		});

		// Sub-object checks
		expectQuery(
			finalizeQuery(
				where("post", `content`)
					.JsonPath("nestedObject")
					.JsonContains({ isBool: true })
					.getQuery(),
			),
		).toEqual({
			text: `
                WHERE "post"."content"->"nestedObject" @> $1::jsonb
            `,
			values: [`{"isBool":true}`],
		});
	});
});

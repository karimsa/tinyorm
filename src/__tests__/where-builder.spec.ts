import { describe, it } from "@jest/globals";
import {
	Column,
	createJoinWhereBuilder,
	createSimpleWhereBuilder,
	Entity,
	sql,
} from "../";
import { expectQuery } from "./util";

describe("WhereBuilder", () => {
	class User extends Entity({ schema: "public", tableName: "users" }) {
		@Column({ type: "uuid" })
		id!: string;
		@Column({ type: "text" })
		name!: string;
		@Column({ type: "text" })
		status!: "Active" | "Inactive";
	}
	class Post extends Entity({ schema: "public", tableName: "posts" }) {
		@Column({ type: "uuid" })
		id!: string;
		@Column({ type: "uuid" })
		author_id!: string;
		@Column({ type: "jsonb" })
		content!: {
			type: "text" | "image";
			value: string;
			nestedObject: { hello: string; isBool: boolean };
			nestedArray: string[];
		};
	}

	describe("JoinWhereBuilder", () => {
		const where = createJoinWhereBuilder<{
			user: User;
			post: Post;
		}>({
			user: User,
			post: Post,
		});

		it("should allow building where clauses", () => {
			// Find all users with a name similar to 'Karim'
			expectQuery(sql.finalize(where("user", "name").Like("%Karim%"))).toEqual({
				text: `
					"user"."name" LIKE $1::text
				`,
				values: ["%Karim%"],
			});

			// Find all users with a name similar to 'Karim' OR is any of ('Bob', 'Alice')
			expectQuery(
				sql.finalize(
					where.or([
						where("user", "name").Like("%Karim%"),
						where("user", "name").EqualsAny(["Bob", "Alice"]),
					]),
				),
			).toEqual({
				text: `
					"user"."name" LIKE $1::text OR "user"."name" = ANY($2::text[])
				`,
				values: ["%Karim%", ["Bob", "Alice"]],
			});
			expectQuery(
				sql.finalize(
					where.and([
						where.or([
							where("user", "name").Like("%Karim%"),
							where("user", "name").EqualsAny(["Bob", "Alice"]),
						]),
						where("user", "status").Equals("Active"),
					]),
				),
			).toEqual({
				text: `
					("user"."name" LIKE $1::text OR "user"."name" = ANY($2::text[]))
					AND "user"."status" = $3::text
				`,
				values: ["%Karim%", ["Bob", "Alice"], "Active"],
			});
		});

		it.only("should allow JSONB queries", () => {
			// Check top-level JSON field value
			expectQuery(
				sql.finalize(
					where("post", sql.json(Post).content.type)
						.CastAs("text")
						.Equals("text"),
				),
			).toEqual({
				text: `
					("post"."content"->"type")::text = $1::text
				`,
				values: ["text"],
			});

			// Check nested JSON field value
			expectQuery(
				sql.finalize(
					where("post", sql.json(Post).content.nestedObject.hello)
						.CastAs("text")
						.Equals("world"),
				),
			).toEqual({
				text: `
					("post"."content"->"nestedObject"->"hello")::text = $1::text
				`,
				values: ["world"],
			});
			expectQuery(
				sql.finalize(
					where("post", sql.json(Post).content.nestedObject.isBool)
						.CastAs("boolean")
						.Equals(true),
				),
			).toEqual({
				text: `
					("post"."content"->"nestedObject"->"isBool")::text::boolean = $1::boolean
				`,
				values: [true],
			});

			// Sub-object checks
			expectQuery(
				sql.finalize(
					where("post", sql.json(Post).content.nestedObject).JsonContains({
						isBool: true,
					}),
				),
			).toEqual({
				text: `
					"post"."content"->"nestedObject" @> $1::jsonb
				`,
				values: [`{"isBool":true}`],
			});
		});
	});

	describe("SingleWhereBuilder", () => {
		const where = createSimpleWhereBuilder(User);

		it("should allow building where clauses", () => {
			// Find all users with a name similar to 'Karim'
			expectQuery(sql.finalize(where("name").Like("%Karim%"))).toEqual({
				text: `
					"name" LIKE $1::text
				`,
				values: ["%Karim%"],
			});

			// Find all users with a name similar to 'Karim' OR is any of ('Bob', 'Alice')
			expectQuery(
				sql.finalize(
					where.or([
						where("name").Like("%Karim%"),
						where("name").EqualsAny(["Bob", "Alice"]),
					]),
				),
			).toEqual({
				text: `
					"name" LIKE $1::text OR "name" = ANY($2::text[])
				`,
				values: ["%Karim%", ["Bob", "Alice"]],
			});
		});
	});
});

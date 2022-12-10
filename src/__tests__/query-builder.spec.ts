import {
	Entity,
	Column,
	sql,
	createSelectBuilder,
	createJoinBuilder,
} from "..";
import { expectQuery, getResolvedType } from "./util";
import { assertType } from "../utils";
import { z } from "zod";

class User extends Entity({ schema: "app", tableName: "user" }) {
	@Column({ type: 'uuid' })
	readonly id: string;
	@Column({ type: 'uuid' })
	readonly status: "Active" | "Inactive";
	@Column({ type: 'text' })
	readonly name: string;
	@Column({ type: 'uuid[]' })
	readonly organization_ids: string[];
}

class UserPost extends Entity({ schema: "app", tableName: "user_post" }) {
	@Column({ type: 'uuid' })
	readonly id: string;
	@Column({ type: 'uuid' })
	readonly user_id: string;
	@Column({ type: 'uuid' })
	readonly post_id: string;
	@Column({ type: 'text' })
	readonly reaction: "Like" | "Dislike" | "Love";
	@Column({ type: 'timestamp with timezone' })
	readonly reacted_at: Date;
}

class Post extends Entity({ schema: "app", tableName: "post" }) {
	@Column({ type: 'uuid' })
	readonly id: string;
	@Column({ type: 'uuid' })
	readonly author_id: string;
	@Column({ type: 'text' })
	readonly content: string;
	@Column({ type: 'timestamp with timezone' })
	readonly created_at: Date;
}

class Organization extends Entity({
	schema: "app",
	tableName: "organization",
}) {
	@Column({ type: 'uuid' })
	readonly id: string;
	@Column({ type: 'text' })
	readonly name: string;
	@Column({ type: 'text' })
	readonly organization_id: string;
}

describe("QueryBuilder", () => {
	describe("Select", () => {
		it("should allow single entity queries", async () => {
			expectQuery(
				createSelectBuilder().from(User).select(["id", "name"]).getQuery(),
			).toEqual({
				text: `SELECT "id", "name" FROM "app"."user"`,
				values: [],
			});
		});
		it("should allow inner joins", async () => {
			expectQuery(
				createJoinBuilder()
					.from(User, "user")
					.select("user", ["id", "name"])
					.getQuery(),
			).toEqual({
				text: `
					SELECT
						"user"."id" AS "user_id", "user"."name" AS "user_name"
					FROM "app"."user" AS "user"
				`,
				values: [],
			});
			expectQuery(
				createJoinBuilder()
					.from(User, "user")
					.innerJoin(
						Organization,
						"organization",
						sql`organization.id = any(user.organization_ids)`,
					)
					.select("user", ["id", "name"])
					.select("organization", ["name"])
					.getQuery(),
			).toEqual({
				text: `
					SELECT
						"user"."id" AS "user_id", "user"."name" AS "user_name",
						"organization"."name" AS "organization_name"
					FROM "app"."user" AS "user"
					INNER JOIN "app"."organization" AS "organization" ON organization.id = any(user.organization_ids)
				`,
				values: [],
			});
			expectQuery(
				createJoinBuilder()
					.from(User, "user")
					.innerJoin(
						Organization,
						"organization",
						sql`organization.id = any(user.organization_ids)`,
					)
					.select("user", ["id", "name"])
					.select("organization", ["name"])
					.where((where) =>
						where.either([
							where("user", "name")
								.Like("%Karim%")
								.andWhere("user", "status")
								.EqualsAny(["Active"]),
							where("organization", "name").Like("Foko"),
						]),
					)
					.getQuery(),
			).toEqual({
				text: `
					SELECT
						"user"."id" AS "user_id", "user"."name" AS "user_name",
						"organization"."name" AS "organization_name"
					FROM "app"."user" AS "user"
					INNER JOIN "app"."organization" AS "organization" ON organization.id = any(user.organization_ids)
					WHERE (("user"."name" LIKE $1::text AND "user"."status" = ANY($2::text[] )) OR ("organization"."name" LIKE $3::text ))
				`,
				values: ["%Karim%", ["Active"], "Foko"],
			});

			assertType<{
				user: { id: string; name: string };
				organization: { name: string };
			} | null>(
				getResolvedType(
					createJoinBuilder()
						.from(User, "user")
						.innerJoin(
							Organization,
							"organization",
							sql`organization.id = any(user.organization_ids)`,
						)
						.select("user", ["id", "name"])
						.select("organization", ["name"]).getOne,
				),
			);
			assertType<
				{
					user: { id: string; name: string };
					organization: { name: string };
				}[]
			>(
				getResolvedType(
					createJoinBuilder()
						.from(User, "user")
						.innerJoin(
							Organization,
							"organization",
							sql`organization.id = any(user.organization_ids)`,
						)
						.select("user", ["id", "name"])
						.select("organization", ["name"]).getMany,
				),
			);
		});
		it("should build results correctly", async () => {
			expect(
				createJoinBuilder()
					.from(User, "user")
					.innerJoin(
						Organization,
						"organization",
						sql`organization.id = any(user.organization_ids)`,
					)
					.select("user", ["id", "name"])
					.select("organization", ["name"])
					.buildOne({
						user_id: "user_id",
						user_name: "user_name",
						organization_name: "organization_name",
					}),
			).toEqual({
				user: { id: "user_id", name: "user_name" },
				organization: {
					name: "organization_name",
				},
			});
			expect(
				createJoinBuilder()
					.from(User, "user")
					.innerJoin(
						Organization,
						"organization",
						sql`organization.id = any(user.organization_ids)`,
					)
					.select("user", ["id", "name"])
					.select("organization", ["name"])
					.buildMany([
						{
							user_id: "user_id",
							user_name: "user_name",
							organization_name: "organization_name",
						},
					]),
			).toEqual([
				{
					user: { id: "user_id", name: "user_name" },
					organization: {
						name: "organization_name",
					},
				},
			]);
		});
		it("should allow custom select values", () => {
			const qb = createJoinBuilder()
				.from(User, "user")
				.innerJoin(UserPost, "user_post", sql`"user_post".user_id = "user".id`)
				.select("user", ["id"])
				.selectRaw(sql`count(distinct user_post.post_id)`, "test", z.number())
				.where((where) => where("user", "name").Equals("Karim"));

			assertType<{ user: { id: string }; test: number } | null>(
				getResolvedType(qb.getOne),
			);
			expect(qb.buildOne({ user_id: "user-id", test: 3.14 })).toEqual({
				user: { id: "user-id" },
				test: 3.14,
			});
			expect(() =>
				qb.buildOne({ user_id: "user-id", test: "3.14" }),
			).toThrowError(/invalid value/i);
			expectQuery(qb.getQuery()).toEqual({
				text: `
					SELECT
						"user"."id" AS "user_id",
						count(distinct user_post.post_id) AS "test"
					FROM "app"."user" AS "user"
					INNER JOIN "app"."user_post" AS "user_post" ON "user_post".user_id = "user".id
					WHERE ("user"."name" = $1::text )
				`,
				values: ["Karim"],
			});
		});
	});
});

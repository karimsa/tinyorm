import {
	Entity,
	Column,
	sql,
	createSelectBuilder,
	createJoinBuilder,
} from "..";
import { expectQuery, getResolvedType } from "./util";
import { assertType } from "../utils";

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
				text: "SELECT id, name FROM app.user",
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
						user.id AS user_id, user.name AS user_name
					FROM app.user AS user
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
						user.id AS user_id, user.name AS user_name,
						organization.name AS organization_name
					FROM app.user AS user
					INNER JOIN app.organization AS organization ON organization.id = any(user.organization_ids)
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
					.andWhere("user", (where) =>
						where("name")
							.Like("%Karim%")
							.orWhere("status")
							.EqualsAny(["Active"]),
					)
					.getQuery(),
			).toEqual({
				text: `
					SELECT
						user.id AS user_id, user.name AS user_name,
						organization.name AS organization_name
					FROM app.user AS user
					INNER JOIN app.organization AS organization ON organization.id = any(user.organization_ids)
					WHERE user.name LIKE $1::text OR user.status = ANY($2::text[] )
				`,
				values: ["%Karim%", ["Active"]],
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
	});
});

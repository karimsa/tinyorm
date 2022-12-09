import {
	Entity,
	Column,
	sql,
	createSelectBuilder,
	createJoinBuilder,
} from "..";
import { expectQuery } from "./util";

class User extends Entity({ schema: "app", tableName: "user" }) {
	@Column({ type: 'uuid' })
	readonly id: string;
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
		});
	});
});

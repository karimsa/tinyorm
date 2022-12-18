import { describe, it } from "@jest/globals";
import { Column, Entity, sql } from "../";
import { ConnectionPool } from "../connection";
import { expectQuery } from "./util";

class TestUser extends Entity({ schema: "public", tableName: "test_user" }) {
	@Column({ type: 'uuid' })
	readonly id!: string;
	@Column({ type: 'text' })
	readonly name!: string;
	@Column({ type: 'jsonb' })
	readonly meta!: { isCool: boolean };
}

describe("Connection", () => {
	it("should allow creating tables", async () => {
		expectQuery(
			sql.finalize(ConnectionPool.getCreateTableQuery(TestUser)),
		).toEqual({
			text: `
				CREATE TABLE IF NOT EXISTS "public"."test_user" (
					"id" uuid NOT NULL,
					"name" text NOT NULL,
					"meta" jsonb NOT NULL
				)
			`,
			values: [],
		});
	});
	it("should allow deleting selectively", async () => {
		const query = await ConnectionPool.getDeleteFromQuery(TestUser, (where) =>
			where("name").Equals("Karim"),
		);
		expectQuery(sql.finalize(query)).toEqual({
			text: `
				DELETE FROM "public"."test_user"
				WHERE "name" = $1::text
			`,
			values: ["Karim"],
		});
	});
});

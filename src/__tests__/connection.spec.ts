import { describe, it } from "@jest/globals";
import { Column, Entity, sql } from "../";
import { ConnectionPool } from "../connection";
import { expectQuery } from "./util";

class TestUser extends Entity({ schema: "public", tableName: "test_user" }) {
	@Column({ type: 'uuid' })
	readonly id: string;
	@Column({ type: 'text' })
	readonly name: string;
	@Column({ type: 'jsonb' })
	readonly meta: { isCool: boolean };
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
	it("should allow inserting new rows", async () => {
		expectQuery(
			sql.finalize(
				ConnectionPool.getInsertQuery(TestUser, {
					id: "6f0dea07-dbf6-4e6b-9e3b-8df47d278628",
					name: "test",
					meta: { isCool: true },
				}),
			),
		).toEqual({
			text: `INSERT INTO "public"."test_user" ("id", "name", "meta") VALUES ($1::text, $2::text, $3::jsonb)`,
			values: [
				"6f0dea07-dbf6-4e6b-9e3b-8df47d278628",
				"test",
				`{"isCool":true}`,
			],
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

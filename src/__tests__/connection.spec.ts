import { describe, it } from "@jest/globals";
import { Entity, Column, finalizeQuery } from "../";
import { createConnectionPool } from "../connection";
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
	it("should allow inserting new rows", async () => {
		const pool = await createConnectionPool({
			port: 5531,
			user: "postgres",
			database: "end_to_end",
		});

		expectQuery(
			finalizeQuery(
				pool.getInsertQuery(TestUser, {
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
});

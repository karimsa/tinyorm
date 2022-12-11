import { describe, it } from "@jest/globals";
import { Entity, Column, Index, createJoinBuilder } from "../";
import { createConnectionPool } from "../connection";
import { expectQuery } from "./util";

describe("Migrations", () => {
	it("should setup table from scratch", async () => {
		@Index(MigrationTestUser)('idx_user_name', ['name'])
		class MigrationTestUser extends Entity({
			schema: "public",
			tableName: "migration_test_user",
		}) {
			@Column({ type: 'uuid' })
			readonly id: string;
			@Column({ type: 'text' })
			readonly name: string;
			@Column({ type: 'jsonb' })
			readonly meta: { isCool: boolean };
		}

		const pool = await createConnectionPool({
			port: 5432,
			user: "postgres",
			password: "postgres",
			database: "postgres",
		});

		await pool.withTransaction(async (connection) => {
			await connection.dropTable(MigrationTestUser);
		});
		await pool.withClient(async (client) => {
			await expect(
				createJoinBuilder()
					.from(MigrationTestUser, "test")
					.selectAll("test")
					.getOne(client),
			).rejects.toThrowError(/relation.*does not exist/);
		});

		const queries = await pool.getMigrationQueries(MigrationTestUser);
		expect(queries).toMatchObject([
			{
				reason: "Missing Table",
				query: expect.any(Object),
			},
			{
				reason: "Missing Index",
				query: expect.any(Object),
			},
		]);

		expectQuery(queries[0].query).toEqual({
			text: `
				CREATE TABLE IF NOT EXISTS "public"."migration_test_user" (
					"id" uuid NOT NULL,
					"name" text NOT NULL,
					"meta" jsonb NOT NULL
				)
			`,
			values: [],
		});
		expectQuery(queries[1].query).toEqual({
			text: `
				CREATE INDEX IF NOT EXISTS "idx_user_name" ON "public"."migration_test_user" ("name")
			`,
			values: [],
		});
		expect(queries).toHaveLength(2);

		await pool.withClient(async (client) => {
			await client.query(queries[0].query);
			await client.query(queries[1].query);

			await expect(
				createJoinBuilder()
					.from(MigrationTestUser, "test")
					.selectAll("test")
					.getOne(client),
			).resolves.toEqual(null);
		});

		await pool.destroy();
	});
});

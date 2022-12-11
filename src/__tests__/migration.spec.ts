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
			await connection.unsafe_resetAllMigrations();
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
				queries: [expect.any(Object)],
			},
			{
				reason: "Missing Index",
				queries: [expect.any(Object)],
			},
		]);

		expectQuery(queries[0].queries[0]).toEqual({
			text: `
				CREATE TABLE IF NOT EXISTS "public"."migration_test_user" (
					"id" uuid NOT NULL,
					"name" text NOT NULL,
					"meta" jsonb NOT NULL
				)
			`,
			values: [],
		});
		expect(queries[0].queries).toHaveLength(1);

		expectQuery(queries[1].queries[0]).toEqual({
			text: `
				CREATE INDEX IF NOT EXISTS "idx_user_name" ON "public"."migration_test_user" ("name")
			`,
			values: [],
		});
		expect(queries[0].queries).toHaveLength(1);

		expect(queries).toHaveLength(2);

		await pool.executeMigration("test", queries);

		await pool.withClient(async (client) => {
			await expect(
				createJoinBuilder()
					.from(MigrationTestUser, "test")
					.selectAll("test")
					.getOne(client),
			).resolves.toEqual(null);
		});

		await pool.destroy();
	});

	it("should generate index changing migrations", async () => {
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
			await connection.unsafe_resetAllMigrations();
			await connection.executeMigration(
				"test",
				await connection.getMigrationQueries(MigrationTestUser),
			);
		});

		@Index(MigrationTestUserUpdated)('idx_user_name', ['id'])
		class MigrationTestUserUpdated extends Entity({
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

		const queries = await pool.getMigrationQueries(MigrationTestUserUpdated);
		expect(queries).toMatchObject([
			{
				reason: "Index Updated",
				queries: [expect.any(Object), expect.any(Object)],
			},
		]);
		expectQuery(queries[0].queries[0]).toEqual({
			text: `
				DROP INDEX IF EXISTS "public"."idx_user_name"
			`,
			values: [],
		});
		expectQuery(queries[0].queries[1]).toEqual({
			text: `
				CREATE INDEX IF NOT EXISTS "idx_user_name" ON "public"."migration_test_user" ("id")
			`,
			values: [],
		});

		await pool.destroy();
	});
});

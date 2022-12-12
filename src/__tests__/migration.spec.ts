import { describe, it } from "@jest/globals";
import { Entity, Column, Index, createJoinBuilder, sql } from "../";
import { ConnectionPool, createConnectionPool } from "../connection";
import { expectQuery } from "./util";
import { EntityFromShape } from "../entity";
import { SuggestedMigration } from "../migrations";

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

		const queries = (await pool.getMigrationQueries(MigrationTestUser))!;
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

		const queries = (await pool.getMigrationQueries(MigrationTestUserUpdated))!;
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

	describe("Column Migrations", () => {
		let pool: ConnectionPool;

		beforeAll(async () => {
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

			pool = await createConnectionPool({
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
		});

		afterAll(async () => {
			await pool.destroy();
		});

		const expectMigrations = async (
			updatedClass: EntityFromShape<unknown>,
			expectedQueries: SuggestedMigration[],
		) => {
			const actualQueries = await pool.getMigrationQueries(updatedClass);
			if (!actualQueries) {
				throw new Error(`Migration generation triggered a rollback`);
			}

			console.dir({ actualQueries, expectedQueries }, { depth: null });
			expect(actualQueries).toMatchObject(
				expectedQueries.map((migration) => ({
					...migration,
					queries: migration.queries.map(() => expect.any(Object)),
				})),
			);

			for (let i = 0; i < expectedQueries.length; i++) {
				for (let j = 0; j < expectedQueries[i].queries.length; j++) {
					expectQuery(actualQueries[i].queries[j]).toEqual(
						expectedQueries[i].queries[j],
					);
				}
			}
		};

		it("should handle column data type update", async () => {
			class MigrationTestUserUpdated extends Entity({
				schema: "public",
				tableName: "migration_test_user",
			}) {
				@Column({ type: 'uuid' })
				readonly id: string;
				@Column({ type: 'uuid' })
				readonly name: string;
				@Column({ type: 'jsonb' })
				readonly meta: { isCool: boolean };
			}

			await expectMigrations(MigrationTestUserUpdated, [
				{
					reason: "Column Type Updated",
					queries: [
						{
							text: `
								ALTER TABLE "public"."migration_test_user" ALTER COLUMN "name" TYPE uuid
							`,
							values: [],
						},
					],
				},
			]);
		});

		it("should handle column default value being added", async () => {
			class MigrationTestUserUpdated extends Entity({
				schema: "public",
				tableName: "migration_test_user",
			}) {
				@Column({ type: 'uuid' })
				readonly id: string;
				@Column({ type: 'text', defaultValue: sql`'test'` })
				readonly name: string;
				@Column({ type: 'jsonb' })
				readonly meta: { isCool: boolean };
			}

			await expectMigrations(MigrationTestUserUpdated, [
				{
					reason: "Column Default Updated",
					queries: [
						{
							text: `
								ALTER TABLE "public"."migration_test_user" ALTER COLUMN "name" SET DEFAULT 'test'
							`,
							values: [],
						},
					],
				},
			]);
		});

		it("should handle column default value being updated", async () => {
			class MigrationTestUserUpdated extends Entity({
				schema: "public",
				tableName: "migration_test_user",
			}) {
				@Column({ type: 'uuid' })
				readonly id: string;
				@Column({ type: 'text', defaultValue: sql`'test'` })
				readonly name: string;
				@Column({ type: 'jsonb' })
				readonly meta: { isCool: boolean };
			}

			await expectMigrations(MigrationTestUserUpdated, [
				{
					reason: "Column Default Updated",
					queries: [
						{
							text: `
								ALTER TABLE "public"."migration_test_user" ALTER COLUMN "name" SET DEFAULT 'test'
							`,
							values: [],
						},
					],
				},
			]);

			// Apply the migrations to the class
			await expect(
				pool.withTransaction(async (connection) => {
					console.dir(
						await connection.getMigrationQueries(MigrationTestUserUpdated),
						{ depth: null },
					);
					await connection.executeMigration(
						"test default value set",
						await connection.getMigrationQueries(MigrationTestUserUpdated),
					);

					// Create a new entity for the same table, but with a different default value
					class MigrationTestUserUpdated2 extends Entity({
						schema: "public",
						tableName: "migration_test_user",
					}) {
						@Column({ type: 'uuid' })
						readonly id: string;
						@Column({ type: 'text', defaultValue: sql`'test2'` })
						readonly name: string;
						@Column({ type: 'jsonb' })
						readonly meta: { isCool: boolean };
					}

					// Verify that the migration is correctly generated
					await expectMigrations(MigrationTestUserUpdated2, [
						{
							reason: "Column Default Updated",
							queries: [
								{
									text: `
									ALTER TABLE "public"."migration_test_user" ALTER COLUMN "name" SET DEFAULT 'test2'
								`,
									values: [],
								},
							],
						},
					]);

					// Rollback the transaction to avoid leaking to other tests
					throw new Error(`rollback`);
				}),
			).rejects.toThrowError(/rollback/);

			// Verify that the same migration is correctly generated, so the state has not applied
			await expectMigrations(MigrationTestUserUpdated, [
				{
					reason: "Column Default Updated",
					queries: [
						{
							text: `
								ALTER TABLE "public"."migration_test_user" ALTER COLUMN "name" SET DEFAULT 'test'
							`,
							values: [],
						},
					],
				},
			]);
		});

		it("should handle column renames", async () => {
			class MigrationTestUserUpdated extends Entity({
				schema: "public",
				tableName: "migration_test_user",
			}) {
				@Column({ type: 'uuid' })
				readonly id: string;
				@Column({ type: 'text', previousName: 'name' })
				readonly foobar: string;
				@Column({ type: 'jsonb' })
				readonly meta: { isCool: boolean };
			}

			await expectMigrations(MigrationTestUserUpdated, [
				{
					reason: "Column Renamed",
					queries: [
						{
							text: `
								ALTER TABLE "public"."migration_test_user" RENAME COLUMN "name" TO "foobar"
							`,
							values: [],
						},
					],
				},
			]);
		});
	});
});

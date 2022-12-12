import { Connection, ConnectionPool } from "./connection";
import { EntityFromShape, getEntityFields, getEntityIndices } from "./entity";
import { logger } from "./logger";
import {
	SchemaCatalog,
	TableCatalog,
	TableColumnCatalog,
	TableIndexCatalog,
} from "./pgcatalog";
import { FinalizedQuery, finalizeQuery, sql } from "./queries";
import { createJoinBuilder } from "./query-builder";

export type MigrationReason =
	| "Missing Schema"
	| "Missing Table"
	| "Missing Index"
	| "Unused Index"
	| "New Index"
	| "Index Updated"
	| "Unused Column"
	| "New Column"
	| "Column Default Updated"
	| "Column Type Updated"
	| "Column Renamed";

export interface SuggestedMigration {
	reason: MigrationReason;
	queries: FinalizedQuery[];
}

export class MigrationGenerator {
	constructor(readonly connection: Connection) {}

	async getSchemaMigration(schemaName: string) {
		const migrationQueries: SuggestedMigration[] = [];

		const schemaInfo = await createJoinBuilder()
			.from(SchemaCatalog, "schema_entry")
			.selectAll("schema_entry")
			.where((where) => where("schema_entry", "schema_name").Equals(schemaName))
			.getOne(this.connection);

		// Creation of schema
		if (!schemaInfo) {
			migrationQueries.push({
				reason: "Missing Schema",
				queries: [
					finalizeQuery(
						sql`CREATE SCHEMA IF NOT EXISTS "${sql.asUnescaped(schemaName)}"`,
					),
				],
			});
		}

		return migrationQueries;
	}

	async getTableInitMigration(entity: EntityFromShape<unknown>) {
		const migrationQueries: SuggestedMigration[] = [];

		migrationQueries.push({
			reason: "Missing Table",
			queries: [
				finalizeQuery(ConnectionPool.getCreateTableQuery(entity, false)),
			],
		});

		for (const query of getEntityIndices(entity).values()) {
			migrationQueries.push({
				reason: "Missing Index",
				queries: [query],
			});
		}

		return migrationQueries;
	}

	async getTableIndicesMigrations(entity: EntityFromShape<unknown>) {
		const migrationQueries: SuggestedMigration[] = [];
		const indexSet = getEntityIndices(entity);

		const existingIndexData = await createJoinBuilder()
			.from(TableIndexCatalog, "index_entry")
			.selectAll("index_entry")
			.where((where) =>
				where("index_entry", "schemaname")
					.Equals(entity.schema)
					.andWhere("index_entry", "tablename")
					.Equals(entity.tableName),
			)
			.getMany(this.connection);

		for (const index of existingIndexData) {
			const currentIndex = indexSet.get(index.index_entry.indexname);
			if (currentIndex) {
				// Indices that had their criteria changed
				if (index.index_entry.indexdef !== currentIndex.text) {
					migrationQueries.push({
						reason: "Index Updated",
						queries: [
							finalizeQuery(
								sql`DROP INDEX IF EXISTS "${sql.asUnescaped(
									entity.schema,
								)}"."${sql.asUnescaped(index.index_entry.indexname)}"`,
							),
							currentIndex,
						],
					});
				}
			} else {
				// Indices that need to be dropped
				migrationQueries.push({
					reason: "Unused Index",
					queries: [
						finalizeQuery(
							sql`DROP INDEX IF EXISTS "${sql.asUnescaped(
								entity.schema,
							)}"."${sql.asUnescaped(index.index_entry.indexname)}"`,
						),
					],
				});
			}
		}

		// Look for indices that are brand new
		for (const [indexName, indexQuery] of indexSet.entries()) {
			if (
				!existingIndexData.find(
					(row) => row.index_entry.indexname === indexName,
				)
			) {
				migrationQueries.push({
					reason: "New Index",
					queries: [indexQuery],
				});
			}
		}

		return migrationQueries;
	}

	async getTableColumnMigrations(entity: EntityFromShape<unknown>) {
		const migrations: SuggestedMigration[] = [];

		const columnSet = getEntityFields(entity);
		const existingColumnData = await createJoinBuilder()
			.from(TableColumnCatalog, "col")
			.selectAll("col")
			.where((where) =>
				where("col", "table_schema")
					.Equals(entity.schema)
					.andWhere("col", "table_name")
					.Equals(entity.tableName),
			)
			.getMany(this.connection);

		const validExistingColumnNames = new Set<string>();

		for (const [columnName, columnOptions] of columnSet.entries()) {
			const existingColumns = existingColumnData.filter(
				(column) =>
					column.col.column_name === columnName ||
					column.col.column_name === columnOptions.previousName,
			);
			if (existingColumns.length > 1) {
				throw new Error(
					`Multiple existing columns matched the description for ${
						entity.tableName
					}.${columnName}: ${JSON.stringify(
						existingColumns.map((col) => col.col.column_name),
					)}`,
				);
			}

			const matchingStoredColumn = existingColumns[0]?.col;
			if (matchingStoredColumn) {
				validExistingColumnNames.add(matchingStoredColumn.column_name);

				// Columns that need to be renamed
				if (matchingStoredColumn.column_name !== columnName) {
					migrations.push({
						reason: "Column Renamed",
						queries: [
							finalizeQuery(
								sql`ALTER TABLE "${sql.asUnescaped(
									entity.schema,
								)}"."${sql.asUnescaped(
									entity.tableName,
								)}" RENAME COLUMN "${sql.asUnescaped(
									matchingStoredColumn.column_name,
								)}" TO "${sql.asUnescaped(columnName)}"`,
							),
						],
					});
				}

				// Columns that don't have a default value anymore
				if (
					matchingStoredColumn.column_default &&
					!columnOptions.defaultValue
				) {
					migrations.push({
						reason: "Column Default Updated",
						queries: [
							finalizeQuery(
								sql`ALTER TABLE "${sql.asUnescaped(
									entity.schema,
								)}"."${sql.asUnescaped(
									entity.tableName,
								)}" ALTER COLUMN "${sql.asUnescaped(
									matchingStoredColumn.column_name,
								)}" DROP DEFAULT`,
							),
						],
					});
				}

				// Columns that changed their default values
				else if (
					columnOptions.defaultValue &&
					matchingStoredColumn.column_default !==
						columnOptions.defaultValue?.text.join("")
				) {
					migrations.push({
						reason: "Column Default Updated",
						queries: [
							finalizeQuery(
								sql`ALTER TABLE "${sql.asUnescaped(
									entity.schema,
								)}"."${sql.asUnescaped(
									entity.tableName,
								)}" ALTER COLUMN "${sql.asUnescaped(
									matchingStoredColumn.column_name,
								)}" SET DEFAULT ${columnOptions.defaultValue}`,
							),
						],
					});
				}

				// Columns that changed their types
				if (matchingStoredColumn.data_type !== columnOptions.type) {
					migrations.push({
						reason: "Column Type Updated",
						queries: [
							finalizeQuery(
								sql`ALTER TABLE "${sql.asUnescaped(
									entity.schema,
								)}"."${sql.asUnescaped(
									entity.tableName,
								)}" ALTER COLUMN "${sql.asUnescaped(
									matchingStoredColumn.column_name,
								)}" TYPE ${sql.asUnescaped(columnOptions.type)}`,
							),
						],
					});
				}
			}
		}

		// Columns that need to be dropped
		for (const existingColumn of existingColumnData) {
			if (!validExistingColumnNames.has(existingColumn.col.column_name)) {
				migrations.push({
					reason: "Unused Column",
					queries: [
						finalizeQuery(
							sql`ALTER TABLE "${sql.asUnescaped(
								entity.schema,
							)}"."${sql.asUnescaped(
								entity.tableName,
							)}" DROP COLUMN "${sql.asUnescaped(
								existingColumn.col.column_name,
							)}"`,
						),
					],
				});
			}
		}

		return migrations;
	}

	async getMigrationQueries(
		entity: EntityFromShape<unknown>,
	): Promise<SuggestedMigration[]> {
		const migrationQueries: SuggestedMigration[] = [
			...(await this.getSchemaMigration(entity.schema)),
		];

		// Creation of table from scratch
		const tableInfo = await createJoinBuilder()
			.from(TableCatalog, "table_entry")
			.selectAll("table_entry")
			.where((where) =>
				where("table_entry", "table_schema")
					.Equals(entity.schema)
					.andWhere("table_entry", "table_name")
					.Equals(entity.tableName),
			)
			.getOne(this.connection);
		if (!tableInfo) {
			return [
				...migrationQueries,
				...(await this.getTableInitMigration(entity)),
			];
		}

		// Identify table changes
		migrationQueries.push(...(await this.getTableIndicesMigrations(entity)));
		migrationQueries.push(...(await this.getTableColumnMigrations(entity)));

		return migrationQueries;
	}
}

export interface Migration {
	name: string;
	createdAt: string;
	queries: (FinalizedQuery | SuggestedMigration)[];
}

export async function runMigrations(
	pool: ConnectionPool,
	migrations: Migration[],
) {
	if (migrations.length === 0) {
		return;
	}

	const sortedMigrations = [...migrations].sort((left, right) =>
		left.createdAt.localeCompare(right.createdAt),
	);

	logger.info(`Initializing migrations table`);
	await pool.withTransaction(async (connection) => {
		await connection.initMigrations();
	});
	logger.info(`Ready to run migrations`);

	for (const migration of sortedMigrations) {
		logger.info(
			{ migrationName: migration.name, createdAt: migration.createdAt },
			`Starting migration`,
		);
		await pool.withTransaction(async (connection) => {
			await connection.executeMigration(migration.name, migration.queries);
		});
		logger.info(
			{ migrationName: migration.name },
			`Migration completed successfully`,
		);
	}
}

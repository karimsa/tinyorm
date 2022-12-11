import { Entity, Column } from "./entity";

// This entity represents the shape of postgresql's internal information_schema.schemata table
export class SchemaCatalog extends Entity({
	schema: "information_schema",
	tableName: "schemata",
}) {
	@Column({ type: 'text' })
	readonly schema_name: string;
}

export class TableCatalog extends Entity({
	schema: "information_schema",
	tableName: "tables",
}) {
	@Column({ type: 'text' })
	readonly table_schema: string;

	@Column({type:'text'})
	readonly table_name: string;
}

export class TableColumnCatalog extends Entity({
	schema: "information_schema",
	tableName: "columns",
}) {
	@Column({ type: 'text' })
	readonly table_schema: string;

	@Column({type:'text'})
	readonly table_name: string;

	@Column({type:'text'})
	readonly column_name: string;

	@Column({type:'text'})
	readonly is_nullable: string;

	@Column({type:'text'})
	readonly column_default: string;

	@Column({type:'text'})
	readonly data_type: string;
}

export class TableIndexCatalog extends Entity({
	schema: "pg_catalog",
	tableName: "pg_indexes",
}) {
	@Column({type:'text'})
	readonly schemaname: string;

	@Column({type:'text'})
	readonly tablename: string;

	@Column({type:'text'})
	readonly indexname: string;

	@Column({type:'text'})
	readonly indexdef: string;
}

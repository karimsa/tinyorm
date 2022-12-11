import { Entity, Column } from "./entity";

export class SchemaCatalog extends Entity({
	schema: "information_schema",
	tableName: "schemata",
}) {
	@Column({type:'text'})
	readonly schema_name: string;
}

export class TableCatalog extends Entity({
	schema: "information_schema",
	tableName: "tables",
}) {
	@Column({ type: 'text' })
	table_schema: string;

	@Column({type:'text'})
	table_name: string;
}

export class TableColumnCatalog extends Entity({
	schema: "information_schema",
	tableName: "columns",
}) {
	@Column({ type: 'text' })
	table_schema: string;

	@Column({type:'text'})
	table_name: string;

	@Column({type:'text'})
	column_name: string;

	@Column({type:'text'})
	is_nullable: string;

	@Column({type:'text'})
	column_default: string;

	@Column({type:'text'})
	data_type: string;
}

export class TableIndexCatalog extends Entity({
	schema: "pg_catalog",
	tableName: "pg_indexes",
}) {
	@Column({type:'text'})
	schemaname: string;

	@Column({type:'text'})
	tablename: string;

	@Column({type:'text'})
	indexname: string;

	@Column({type:'text'})
	indexdef: string;
}

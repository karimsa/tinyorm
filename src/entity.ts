import {
	FinalizedQuery,
	isJsonRef,
	JsonRef,
	PostgresBooleanColumnType,
	PostgresDateColumnType,
	PostgresJsonColumnType,
	PostgresNumericColumnType,
	PostgresStringColumnType,
	PreparedQuery,
	readJsonRef,
	sql,
	unsafeFlattenQuery,
} from "./queries";
import { assertCase } from "./utils";

const Registry = process.env.NODE_ENV === "test" ? Map : WeakMap;

const fieldRegistry = new Registry<object, Map<string, ColumnStoredOptions>>();
const keyRegistry = new Registry<object, EntityKey[]>();

/**
 * Factory for creating base classes for entities.
 *
 * @pageCategory Decorators
 * @param options.schema the database schema within which the entity will live (defaults to "public")
 * @param options.tableName the name of the table in which the entity will live
 * @returns a base class that you must extend to create your entity
 */
export function Entity(options: { schema?: string; tableName: string }) {
	const { schema, tableName } = options;

	if (schema) {
		assertCase("schema", schema);
	}
	assertCase("tableName", tableName);

	return class {
		static readonly schema = schema ?? "public";
		static readonly tableName = tableName;
	};
}

const indexRegistry = new Registry<
	object,
	Map<string, { query: FinalizedQuery; previousName?: string }>
>();

/**
 * Decorator for defining an index on an entity.
 *
 * @pageCategory Decorators
 * @param entity any tinyorm entity
 */
export function Index<Shape>(
	entity: EntityFromShape<Shape>,
): (
	name: string,
	columns: PreparedQuery | ((string & keyof Shape) | JsonRef<Shape>)[],
	options?: { unique?: boolean; previousName?: string },
) => (target: EntityFromShape<Shape>) => void {
	return (name, columns, options) => {
		return (target) => {
			const indexQuery = unsafeFlattenQuery(
				Array.isArray(columns)
					? sql.unescaped(
							`(${columns
								.map((column) =>
									isJsonRef(column) ? readJsonRef(column) : `"${column}"`,
								)
								.join(", ")})`,
					  )
					: columns,
			);
			const finalizedQuery = sql.finalize(
				sql`CREATE${sql.asUnescaped(
					options?.unique ? " UNIQUE" : "",
				)} INDEX IF NOT EXISTS "${sql.asUnescaped(
					name,
				)}" ON ${entity} ${indexQuery}`,
			);

			const indexSet =
				indexRegistry.get(target) ??
				new Map<string, { query: FinalizedQuery; previousName?: string }>();
			indexRegistry.set(target, indexSet);

			if (indexSet.has(name)) {
				throw new Error(
					`Index '${name}' on '${entity.tableName}' was specified twice`,
				);
			}
			indexSet.set(name, {
				query: finalizedQuery,
				previousName: options?.previousName,
			});
		};
	};
}

export type PostgresColumnType =
	| PostgresStringColumnType
	| PostgresBooleanColumnType
	| PostgresDateColumnType
	| PostgresNumericColumnType
	| PostgresJsonColumnType;

export interface ColumnStoredOptions {
	type: PostgresColumnType | `${PostgresColumnType}[]`;
	nullable?: boolean;
	defaultValue?: PreparedQuery;
	previousName?: string;
}

/**
 * Passed as options to {@link Column} decorator.
 */
export interface ColumnOptions {
	/**
	 * The datatype of the column in postgres.
	 *
	 * ```ts
	 * class User {
	 * 	@Column({ type: "text" })
	 * 	readonly id!: string;
	 * }
	 * ```
	 */
	type: PostgresColumnType | `${PostgresColumnType}[]`;
	/**
	 * Specifies whether the column can hold null values (defaults to false).
	 *
	 * ```ts
	 * class User {
	 * 	@Column({ type: "text", nullable: true })
	 * 	readonly id!: string | null;
	 * }
	 * ```
	 */
	nullable?: boolean;
	/**
	 * Contains a query that defines the default value of the column.
	 *
	 * ```ts
	 * class User {
	 *  @Column({ type: "text", defaultValue: sql`uuid_generate_v4()` })
	 *  readonly id!: string;
	 * }
	 * ```
	 */
	defaultValue?: PreparedQuery;
	/**
	 * Useful for when you want to rename a column that already exists (used by the migration generator to define table renames).
	 *
	 * ```ts
	 * class User {
	 *  @Column({ type: "text", previousName: "id" })
	 *  readonly uuid!: string;
	 * }
	 * ```
	 */
	previousName?: string;
}

/**
 * Decorator that defines a column on an entity, as described by {@link ColumnOptions}.
 *
 * @pageCategory Decorators
 */
export function Column(options: ColumnOptions) {
	return function (target: object, propertyKey: string) {
		assertCase("property name", propertyKey);

		const defaultValue = options.defaultValue
			? unsafeFlattenQuery(options.defaultValue)
			: undefined;
		const fieldSet =
			fieldRegistry.get(target) ?? new Map<string, ColumnStoredOptions>();
		fieldRegistry.set(target, fieldSet);
		fieldSet.set(propertyKey, {
			...options,
			defaultValue,
		});
	};
}

export interface EntityForeignKey {
	type: "foreign";
	columnName: string;
	refEntity: EntityFromShape<unknown>;
	refColumn: string;
}
export interface EntityPrimaryKey {
	type: "primary";
	columnName: string;
}

export type EntityKey = EntityPrimaryKey | EntityForeignKey;

export function PrimaryKey() {
	return function (target: object, propertyKey: string) {
		assertCase("property name", propertyKey);

		const keySet = keyRegistry.get(target) ?? [];
		keyRegistry.set(target, keySet);
		keySet.push({
			type: "primary",
			columnName: propertyKey,
		});
	};
}

export function ForeignKey<ForeignShape>(
	foreignEntity: EntityFromShape<ForeignShape>,
	foreignColumn: string & keyof ForeignShape,
) {
	return function (target: object, propertyKey: string) {
		const keySet = keyRegistry.get(target) ?? [];
		keyRegistry.set(target, keySet);
		keySet.push({
			type: "foreign",
			columnName: propertyKey,
			refEntity: foreignEntity,
			refColumn: foreignColumn,
		});
	};
}

export type EntityFromShape<Shape> = {
	schema: string;
	tableName: string;
	new (...args: unknown[]): Shape;
};
export type ShapeFromEntity<E> = E extends EntityFromShape<infer Shape>
	? Shape
	: never;

// rome-ignore lint/suspicious/noExplicitAny: This is a type-guard.
export function isEntity(entity: any): entity is EntityFromShape<unknown> {
	return (
		typeof entity === "function" &&
		entity !== null &&
		typeof entity.schema === "string" &&
		typeof entity.tableName === "string"
	);
}

export function getEntityFields(entity: EntityFromShape<unknown>) {
	const fieldSet = fieldRegistry.get(entity.prototype);
	if (!fieldSet) {
		throw new Error(
			`Failed to find field set for entity with name '${entity.tableName}'`,
		);
	}
	if (fieldSet.size === 0) {
		throw new Error(
			`Found empty field set for entity with name '${entity.tableName}'`,
		);
	}
	return fieldSet;
}

export function getEntityKeys(entity: EntityFromShape<unknown>) {
	return keyRegistry.get(entity.prototype);
}

export function getEntityIndices(entity: EntityFromShape<unknown>) {
	return (
		indexRegistry.get(entity) ??
		new Map<string, { query: FinalizedQuery; previousName?: string }>()
	);
}

import {
	FinalizedQuery,
	finalizeQuery,
	isJsonRef,
	joinAllQueries,
	JsonRef,
	PostgresBooleanColumnType,
	PostgresDateColumnType,
	PostgresJsonColumnType,
	PostgresNumericColumnType,
	PostgresStringColumnType,
	PreparedQuery,
	readJsonRef,
	sql,
} from "./queries";
import { assertCase } from "./utils";

const Registry = process.env.NODE_ENV === "test" ? Map : WeakMap;

const fieldRegistry = new Registry<object, Map<string, ColumnStoredOptions>>();

export function Entity({
	schema,
	tableName,
}: { schema?: string; tableName: string }) {
	if (schema) {
		assertCase("schema", schema);
	}
	assertCase("tableName", tableName);

	return class {
		static readonly schema = schema ?? "public";
		static readonly tableName = tableName;
	};
}

const indexRegistry = new Registry<object, Map<string, FinalizedQuery>>();

export function Index<Shape>(
	entity: EntityFromShape<Shape>,
): (
	name: string,
	columns: PreparedQuery | ((string & keyof Shape) | JsonRef<Shape>)[],
	options?: { unique: boolean },
) => (target: EntityFromShape<Shape>) => void {
	return (name, columns, options) => {
		return (target) => {
			const indexQuery = Array.isArray(columns)
				? sql.unescaped(
						`(${columns
							.map((column) =>
								isJsonRef(column) ? readJsonRef(column) : `"${column}"`,
							)
							.join(", ")})`,
				  )
				: columns;
			const finalizedQuery = finalizeQuery(
				joinAllQueries([
					sql`CREATE${sql.asUnescaped(
						options?.unique ? " UNIQUE" : "",
					)} INDEX IF NOT EXISTS "${sql.asUnescaped(name)}" ON ${entity} `,
					indexQuery,
				]),
			);
			if (finalizedQuery.values.length > 0) {
				throw new Error(
					`Index '${name}' on '${entity.tableName}' is invalid: your index must not contain any prepared variables`,
				);
			}

			const indexSet =
				indexRegistry.get(target) ?? new Map<string, FinalizedQuery>();
			indexRegistry.set(target, indexSet);

			if (indexSet.has(name)) {
				throw new Error(
					`Index '${name}' on '${entity.tableName}' was specified twice`,
				);
			}
			indexSet.set(name, finalizedQuery);
		};
	};
}

type PostgresColumnType =
	| PostgresStringColumnType
	| PostgresBooleanColumnType
	| PostgresDateColumnType
	| PostgresNumericColumnType
	| PostgresJsonColumnType;

export interface ColumnStoredOptions {
	type: PostgresColumnType | `${PostgresColumnType}[]`;
	nullable?: boolean;
	defaultValue?: PreparedQuery;
}

export interface ColumnOptions {
	type: PostgresColumnType | `${PostgresColumnType}[]`;
	nullable?: boolean;
	defaultValue?: PreparedQuery;
}

export function Column(options: ColumnOptions) {
	return function (target: object, propertyKey: string) {
		assertCase("property name", propertyKey);

		if (Number(options.defaultValue?.params.length) > 0) {
			throw new Error(
				`Column '${propertyKey}' has a default value that contains prepared variables`,
			);
		}

		const fieldSet =
			fieldRegistry.get(target) ?? new Map<string, ColumnStoredOptions>();
		fieldRegistry.set(target, fieldSet);
		fieldSet.set(propertyKey, {
			...options,
			defaultValue: options.defaultValue ? options.defaultValue : undefined,
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

export function getEntityIndices(entity: EntityFromShape<unknown>) {
	return indexRegistry.get(entity) ?? new Map<string, FinalizedQuery>();
}

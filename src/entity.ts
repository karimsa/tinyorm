import { PostgresValueType } from "./queries";
import { assertCase } from "./utils";

const fieldRegistry = new WeakMap<object, Map<string, ColumnOptions>>();

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

export interface ColumnOptions {
	type: string;
	nullable?: boolean;
}

export function Column(options: ColumnOptions) {
	return function (target: object, propertyKey: string) {
		assertCase("property name", propertyKey);

		const fieldSet = fieldRegistry.get(target) ?? new Map();
		fieldSet.set(propertyKey, options);
		fieldRegistry.set(target, fieldSet);
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

export function getEntityFields<Shape>(entity: EntityFromShape<Shape>) {
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
	return fieldSet as unknown as Map<string & keyof Shape, ColumnOptions>;
}

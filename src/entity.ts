import { assertCase } from "./utils";

const entityRegistry = new WeakMap<
	object,
	{ schema: string; tableName: string; fields: Map<string, ColumnOptions> }
>();
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
		static readonly fieldSet =
			fieldRegistry.get(Object.getPrototypeOf(this)) ?? new Map();
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
	fieldSet: Map<string, ColumnOptions>;
	new (...args: unknown[]): Shape;
};
export type ShapeFromEntity<E> = E extends EntityFromShape<infer Shape>
	? Shape
	: never;

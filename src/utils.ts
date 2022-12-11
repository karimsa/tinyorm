import snakeCase from "lodash.snakecase";
import { EntityFromShape } from "./entity";
import { PostgresSimpleValueType } from "./queries";

export const assertCase = (name: string, value: string) => {
	if (snakeCase(value) !== value) {
		throw new Error(
			`Unexpected non-snakeCase ${name} in entity: '${value}' (expected: ${snakeCase(
				value,
			)})`,
		);
	}
};

export function assertType<T>(value: T) {}

export function isElementOfArray(elm: unknown, list: readonly unknown[]) {
	return list.includes(elm);
}

export type JsonKeys<Shape extends object> = {
	[K in keyof Shape]: K extends string
		? Shape[K] extends
				| Exclude<PostgresSimpleValueType, Date | object>
				| unknown[]
				| null
			? K
			: Shape[K] extends object
			? K | `${K}.${JsonKeys<Shape[K]>}`
			: never
		: never;
}[keyof Shape];

export type EntityJsonKeys<Entity> = Entity extends EntityFromShape<infer Shape>
	? Shape extends object
		? JsonKeys<Shape>
		: never
	: never;

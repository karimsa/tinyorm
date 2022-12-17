import snakeCase from "lodash.snakecase";
import { EventEmitter } from "stream";
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

export interface TypeSafeEventEmitter<
	EventHandlers extends Record<string, object>,
> {
	on<Event extends keyof EventHandlers>(
		eventName: Event,
		handler: (data: EventHandlers[Event]) => void,
	): void;
	off<Event extends keyof EventHandlers>(
		eventName: Event,
		handler: (data: EventHandlers[Event]) => void,
	): void;
	emit<Event extends keyof EventHandlers>(
		eventName: Event,
		data: EventHandlers[Event],
	): void;
}

export function createEventEmitter<
	EventHandlers extends Record<string, object>,
>() {
	return new EventEmitter() as unknown as TypeSafeEventEmitter<EventHandlers>;
}

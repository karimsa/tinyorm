import snakeCase from "lodash.snakecase";
import { EntityFromShape } from "./entity";

export const assertCase = (name: string, value: string) => {
	if (snakeCase(value) !== value) {
		throw new Error(
			`Unexpected non-snakeCase ${name} in entity: '${value}' (expected: ${snakeCase(
				value,
			)})`,
		);
	}
};

export const getEntityRef = (
	entity: EntityFromShape<unknown>,
	alias?: string,
) => {
	if (alias) {
		return `${entity.schema}.${entity.tableName} AS ${alias}`;
	}
	return `${entity.schema}.${entity.tableName}`;
};

export function assertType<T>(value: T) {}

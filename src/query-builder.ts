import { EntityFromShape } from "./entity";
import {
	FinalizedQuery,
	finalizeQuery,
	joinQueries,
	joinAllQueries,
	PreparedQuery,
} from "./queries";
import { assertCase } from "./utils";

class QueryBuilder<Shape extends object, ResultShape> {
	readonly selectedFields: string[] = [];

	constructor(readonly targetFromEntity: EntityFromShape<Shape>) {}

	select<Keys extends string & keyof Shape>(
		keys: Keys[],
	): QueryBuilder<Shape, ResultShape & Pick<Shape, Keys>> {
		this.selectedFields.push(...keys);
		// rome-ignore lint/suspicious/noExplicitAny: <explanation>
		return this as any;
	}

	getQuery(): FinalizedQuery {
		return {
			text: `
                SELECT ${this.selectedFields.join(", ")}
                FROM ${this.targetFromEntity.schema}.${
				this.targetFromEntity.tableName
			}
            `,
			values: [],
		};
	}

	async getOne(): Promise<ResultShape | null> {
		return null;
	}

	async getMany(): Promise<ResultShape[]> {
		return [];
	}
}

class JoinedQueryBuilder<Shapes extends Record<string, object>, ResultShape> {
	readonly selectedFields = new Map<string, string[]>();
	readonly joins: PreparedQuery[] = [];
	readonly includedEntites = new Map<string, EntityFromShape<unknown>>();

	constructor(
		readonly targetFromEntity: EntityFromShape<unknown>,
		readonly targetEntityAlias: string,
	) {}

	innerJoin<Alias extends string, JoinedShape>(
		joinedEntity: EntityFromShape<JoinedShape>,
		alias: Alias,
		condition: PreparedQuery,
	): JoinedQueryBuilder<Shapes & { [key in Alias]: JoinedShape }, ResultShape> {
		assertCase("join alias", alias);
		if (this.includedEntites.has(alias)) {
			throw new Error(
				`Cannot join two entities with same alias (attempted to alias ${joinedEntity.tableName} as ${alias})`,
			);
		}

		this.includedEntites.set(alias, joinedEntity);
		this.joins.push(
			joinQueries(
				{
					text: [
						`INNER JOIN ${joinedEntity.schema}.${joinedEntity.tableName} AS ${alias} ON `,
					],
					params: [],
				},
				condition,
			),
		);
		return this;
	}

	select<
		Alias extends string & keyof Shapes,
		Keys extends string & keyof Shapes[Alias],
	>(
		alias: Alias,
		keys: Keys[],
	): JoinedQueryBuilder<Shapes, ResultShape & Pick<Shapes[Alias], Keys>> {
		const selectedFields = this.selectedFields.get(alias) ?? [];
		this.selectedFields.set(alias, selectedFields);
		selectedFields.push(...keys);
		// rome-ignore lint/suspicious/noExplicitAny: The result has changed at compile-time
		return this as any;
	}

	getSelectedFields() {
		const selectedFields: string[] = [];
		for (const [entityName, fields] of this.selectedFields.entries()) {
			selectedFields.push(
				...fields.map(
					(field) => `${entityName}.${field} AS ${entityName}_${field}`,
				),
			);
		}
		return selectedFields;
	}

	getEntityRef(entity: EntityFromShape<unknown>, alias: string) {
		return `${entity.schema}.${entity.tableName} AS ${alias}`;
	}

	getQuery(): FinalizedQuery {
		return finalizeQuery(
			joinAllQueries([
				{
					text: [
						`
							SELECT ${this.getSelectedFields().join(", ")}
							FROM ${this.getEntityRef(
								this.targetFromEntity,
								this.targetEntityAlias,
							)}
						`,
					],
					params: [],
				},
				...this.joins,
			]),
		);
	}

	async getOne(): Promise<ResultShape | null> {
		return null;
	}

	async getMany(): Promise<ResultShape[]> {
		return [];
	}
}

export function createSelectBuilder() {
	return {
		from<T extends object>(entity: EntityFromShape<T>): QueryBuilder<T, {}> {
			return new QueryBuilder(entity);
		},
	};
}

export function createJoinBuilder() {
	return {
		from<Alias extends string, T extends object>(
			entity: EntityFromShape<T>,
			alias: Alias,
		): JoinedQueryBuilder<{ [key in Alias]: T }, {}> {
			return new JoinedQueryBuilder(entity, alias);
		},
	};
}

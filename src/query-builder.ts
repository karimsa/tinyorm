import { EntityFromShape } from "./entity";
import {
	FinalizedQuery,
	finalizeQuery,
	joinQueries,
	joinAllQueries,
	PreparedQuery,
	PostgresValueType,
} from "./queries";
import { assertCase, getEntityRef } from "./utils";
import { Pool as PostgresClientPool, PoolClient as PostgresClient } from "pg";
import {
	AndWhereQueryBuilder,
	InternalWhereBuilder,
	OrWhereQueryBuilder,
	WhereQueryBuilder,
} from "./comparator";

export class QueryError extends Error {
	constructor(
		message: string,
		private readonly query: FinalizedQuery,
		private readonly internalError: unknown,
	) {
		super(message);
	}
}

abstract class BaseQueryBuilder<ResultShape> {
	abstract buildOne(row: unknown): ResultShape | null;
	abstract getQuery(): FinalizedQuery;

	buildMany(rows: unknown[]): ResultShape[] {
		return rows
			.map((row) => this.buildOne(row))
			.filter((row): row is ResultShape => !!row);
	}

	private async executeQuery(client: PostgresClient, query: FinalizedQuery) {
		try {
			const { rows } = await client.query(query);
			return rows;
		} catch (err: unknown) {
			throw new QueryError(
				err instanceof Error ? String(err.message) : "Query failed",
				query,
				err,
			);
		}
	}

	async getOne(pool: PostgresClientPool): Promise<ResultShape | null> {
		const client = await pool.connect();

		const query = this.getQuery();
		query.text += " LIMIT 1";

		const rows = await this.executeQuery(client, query);
		client.release();
		return this.buildOne(rows[0]);
	}

	async getOneOrFail(pool: PostgresClientPool): Promise<ResultShape> {
		const result = await this.getOne(pool);
		if (!result) {
			throw new Error("Failed to find any results to query");
		}
		return result;
	}

	async getMany(pool: PostgresClientPool): Promise<ResultShape[]> {
		const client = await pool.connect();

		const query = this.getQuery();
		query.text += " LIMIT 1";

		const rows = await this.executeQuery(client, query);
		client.release();
		return this.buildMany(rows);
	}
}

class QueryBuilder<
	Shape extends object,
	ResultShape,
> extends BaseQueryBuilder<ResultShape> {
	readonly #selectedFields: string[] = [];

	constructor(readonly targetFromEntity: EntityFromShape<Shape>) {
		super();
	}

	select<Keys extends string & keyof Shape>(
		keys: Keys[],
	): QueryBuilder<Shape, ResultShape & Pick<Shape, Keys>> {
		this.#selectedFields.push(...keys);
		// rome-ignore lint/suspicious/noExplicitAny: <explanation>
		return this as any;
	}

	getQuery(): FinalizedQuery {
		return {
			text: `
                SELECT ${this.#selectedFields.join(", ")}
                FROM ${getEntityRef(this.targetFromEntity)}
            `,
			values: [],
		};
	}

	buildOne(row: unknown): ResultShape | null {
		if (row === null) {
			return null;
		}
		if (typeof row !== "object") {
			throw new Error("Unexpected row received in query result");
		}

		const resultBuilder: Record<string, unknown> = {};
		const castedRow = row as unknown as Record<string, unknown>;

		for (const field of this.#selectedFields) {
			const value = castedRow[field];
			resultBuilder[field] = value;
		}

		return resultBuilder as unknown as ResultShape;
	}
}

type EntityAlias<Alias, Shapes extends Record<string, object>> = Alias &
	(Alias extends keyof Shapes
		? { invalid: "Cannot reuse an existing alias" }
		: {});

class JoinedQueryBuilder<
	Shapes extends Record<string, object>,
	ResultShape,
> extends BaseQueryBuilder<ResultShape> {
	readonly #selectedFields = new Map<string, string[]>();
	readonly #joins: PreparedQuery[] = [];
	readonly #conditions: PreparedQuery[] = [];
	readonly #includedEntites = new Map<string, EntityFromShape<unknown>>();

	constructor(
		readonly targetFromEntity: EntityFromShape<unknown>,
		readonly targetEntityAlias: string,
	) {
		super();
		this.#includedEntites.set(targetEntityAlias, targetFromEntity);
	}

	innerJoin<Alias extends string, JoinedShape>(
		joinedEntity: EntityFromShape<JoinedShape>,
		alias: EntityAlias<Alias, Shapes>,
		condition: PreparedQuery,
	): JoinedQueryBuilder<Shapes & { [key in Alias]: JoinedShape }, ResultShape> {
		assertCase("join alias", alias);
		if (this.#includedEntites.has(alias)) {
			throw new Error(
				`Cannot join two entities with same alias (attempted to alias ${joinedEntity.tableName} as ${alias})`,
			);
		}

		this.#includedEntites.set(alias, joinedEntity);
		this.#joins.push(
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
	): JoinedQueryBuilder<
		Shapes,
		ResultShape & { [key in Alias]: Pick<Shapes[Alias], Keys> }
	> {
		const selectedFields = this.#selectedFields.get(alias) ?? [];
		this.#selectedFields.set(alias, selectedFields);
		selectedFields.push(...keys);
		// rome-ignore lint/suspicious/noExplicitAny: The result has changed at compile-time
		return this as any;
	}

	#getSelectedFields() {
		const selectedFields: string[] = [];
		for (const [entityName, fields] of this.#selectedFields.entries()) {
			selectedFields.push(
				...fields.map(
					(field) => `${entityName}.${field} AS ${entityName}_${field}`,
				),
			);
		}
		return selectedFields;
	}

	andWhere<Alias extends string & keyof Shapes>(
		alias: Alias,
		whereBuilder: (
			where: WhereQueryBuilder<Shapes[Alias]>,
		) =>
			| AndWhereQueryBuilder<Shapes[Alias]>
			| OrWhereQueryBuilder<Shapes[Alias]>,
	) {
		const targetEntity = this.#includedEntites.get(alias);
		if (!targetEntity) {
			throw new Error(
				`Unrecognized entity alias: ${alias} (known entities: ${JSON.stringify(
					Array.from(this.#includedEntites.keys()),
				)})`,
			);
		}

		const builder = new InternalWhereBuilder<Shapes[Alias]>(
			targetEntity.tableName,
		);
		const where = whereBuilder(((field: string & keyof Shapes[Alias]) =>
			builder.openWhere(field)) as unknown as WhereQueryBuilder<
			Shapes[Alias]
		>) as unknown as InternalWhereBuilder<Shapes[Alias]>;

		const query = where.getQuery();
		if (this.#conditions.length === 0) {
			this.#conditions.push({
				...query,
				text: [`\nWHERE ${query.text[0]}`, ...query.text.slice(1)],
			});
		} else {
			this.#conditions.push(query);
		}

		return this;
	}

	getQuery(): FinalizedQuery {
		return finalizeQuery(
			joinAllQueries([
				{
					text: [
						`
							SELECT ${this.#getSelectedFields().join(", ")}
							FROM ${getEntityRef(
								this.targetFromEntity,
								this.targetEntityAlias,
							)}
						`,
					],
					params: [],
				},
				...this.#joins,
				...this.#conditions,
			]),
		);
	}

	buildOne(row: unknown): ResultShape | null {
		if (row === null) {
			return null;
		}
		if (typeof row !== "object") {
			throw new Error("Unexpected row received in query result");
		}

		const resultBuilder: Record<string, Record<string, unknown>> = {};
		const castedRow = row as unknown as Record<string, unknown>;

		for (const [entityName, fields] of this.#selectedFields.entries()) {
			resultBuilder[entityName] = {};

			for (const field of fields) {
				const value = castedRow[`${entityName}_${field}`];
				resultBuilder[entityName][field] = value;
			}
		}

		return resultBuilder as unknown as ResultShape;
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

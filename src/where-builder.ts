import {
	EntityFromShape,
	getEntityFieldByName,
	PostgresColumnType,
} from "./entity";
import {
	isJsonRef,
	JsonRef,
	PostgresBooleanColumnType,
	PostgresDateColumnType,
	PostgresNumericColumnType,
	PostgresStringColumnType,
	PreparedQuery,
	readJsonRef,
	sql,
} from "./queries";

export class WhereComparators<T> {
	constructor(
		private readonly columnRef: string,
		private readonly dataType?: string,
	) {}

	private getCastedValue(value: unknown, isArray?: boolean) {
		if (this.dataType) {
			return sql.asCastedValue(
				value as string,
				(isArray ? `${this.dataType}[]` : this.dataType) as PostgresColumnType,
			);
		}
		return sql.asUnescaped(this.columnRef);
	}

	/**
	 * Casts the current column to a boolean.
	 *
	 * ```ts
	 * where('name').CastAs('boolean').Equals(true)
	 * // Generates: WHERE name::boolean = true
	 * ```
	 */
	CastAs(boolean: PostgresBooleanColumnType): WhereComparators<boolean>;
	/**
	 * Casts the current column to a number.
	 *
	 * ```ts
	 * where('name').CastAs('double precision').Equals(3.14)
	 * // Generates: WHERE name::double precision = 3.14
	 * ```
	 */
	CastAs(number: PostgresNumericColumnType): WhereComparators<boolean>;
	/**
	 * Casts the current column to a string.
	 *
	 * ```ts
	 * where('name').CastAs('text').Equals('test')
	 * // Generates: WHERE name::text = 'test'
	 * ```
	 */
	CastAs(string: PostgresStringColumnType): WhereComparators<string>;
	/**
	 * Casts the current column to a date or timestamp type.
	 *
	 * ```ts
	 * where('name').CastAs('date').Equals(new Date())
	 * // Generates: WHERE name::date = '2021-01-01'
	 * ```
	 */
	CastAs(date: PostgresDateColumnType): WhereComparators<Date>;
	/**
	 * Casts the current column to any other postgres type.
	 *
	 * ```ts
	 * where('name').CastAs('some_other_col_type').Equals('test')
	 * // Generates: WHERE name::some_other_col_type = 'test'
	 * ```
	 */
	CastAs(unknown: string): WhereComparators<unknown>;
	CastAs(type: string): WhereComparators<unknown> {
		if (this.dataType === "jsonb" && type !== "text") {
			return new WhereComparators(`(${this.columnRef})::text::${type}`, type);
		}
		return new WhereComparators(
			this.columnRef.includes("->")
				? `(${this.columnRef})::${type}`
				: `${this.columnRef}::${type}`,
			type,
		);
	}

	/**
	 * Performs an exact match comparison.
	 *
	 * ```ts
	 * where('name').Equals('test')
	 * // Generates: WHERE name = 'test'
	 * ```
	 *
	 * @param value value to compare against
	 */
	Equals(value: T & (string | boolean | number | null)) {
		return sql`${sql.asUnescaped(this.columnRef)} = ${this.getCastedValue(
			value,
		)}`;
	}

	/**
	 * Performs an exact non-match comparison.
	 *
	 * ```ts
	 * where('name').NotEquals('test')
	 * // Generates: WHERE name != 'test'
	 * ```
	 *
	 * @param value value to compare against
	 */
	NotEquals(value: T & (string | boolean | number | null)) {
		return sql`${sql.asUnescaped(this.columnRef)} != ${this.getCastedValue(
			value,
		)}`;
	}

	/**
	 * Performs an array contains comparison, checking to see if the given column
	 * contains any of the given values.
	 *
	 * ```ts
	 * where('name').EqualsAny(['test', 'test2'])
	 * // Generates: WHERE name = ANY(array{'test', 'test2'})
	 * ```
	 *
	 * @param values
	 */
	EqualsAny(values: T[]) {
		return sql`${sql.asUnescaped(this.columnRef)} = ANY(${this.getCastedValue(
			values,
			true,
		)})`;
	}

	/**
	 * Performs an array contains comparison, checking to see if the given column
	 * contains none of the given values.
	 *
	 * ```ts
	 * where('name').EqualsNone(['test', 'test2'])
	 * // Generates: WHERE name <> ANY(array{'test', 'test2'})
	 * ```
	 */
	EqualsNone(values: T[]) {
		return sql`${sql.asUnescaped(this.columnRef)} <> ${this.getCastedValue(
			values,
			true,
		)}`;
	}

	/**
	 * Performs a partial text comparison.
	 *
	 * ```ts
	 * where('name').Like('%test%')
	 * // Generates: WHERE name LIKE '%test%'
	 * ```
	 */
	Like(value: T & string) {
		return sql`${sql.asUnescaped(this.columnRef)} LIKE ${this.getCastedValue(
			value,
		)}`;
	}

	/**
	 * Performs a partial text non-match comparison.
	 *
	 * ```ts
	 * where('name').NotLike('%test%')
	 * // Generates: WHERE name NOT LIKE '%test%'
	 * ```
	 */
	NotLike(value: T & string) {
		return sql`${sql.asUnescaped(
			this.columnRef,
		)} NOT LIKE ${this.getCastedValue(value)}`;
	}

	/**
	 * Performs a JSONB sub-object search comparison.
	 *
	 * ```ts
	 * where('data').JsonContains({ test: 'test' })
	 * // Generates: WHERE data @> '{"test": "test"}'
	 * ```
	 *
	 * See [postgres docs](https://www.postgresql.org/docs/9.5/functions-json.html) for more information.
	 */
	JsonContains(subObject: string | Partial<T>) {
		return sql`${sql.asUnescaped(this.columnRef)} @> ${this.getCastedValue(
			JSON.stringify(subObject),
		)}`;
	}

	JsonArrayIncludes(element: string | number | boolean) {
		return sql`${sql.asUnescaped(this.columnRef)} ? ${this.getCastedValue(
			element,
		)}`;
	}
}

const whereUtils = {
	and(conditions: PreparedQuery[]) {
		return sql.join(
			conditions.map((term) =>
				term.text.includes(" AND ") || term.text.includes(" OR ")
					? sql.brackets(term)
					: term,
			),
			sql` AND `,
		);
	},
	or(conditions: PreparedQuery[]) {
		return sql.join(
			conditions.map((term) =>
				term.text.includes(" AND ") || term.text.includes(" OR ")
					? sql.brackets(term)
					: term,
			),
			sql` OR `,
		);
	},
};

export function createSimpleWhereBuilder<Shape>(
	entity: EntityFromShape<Shape>,
) {
	function openWhere<Key extends string & keyof Shape>(column: Key) {
		const columnOptions = getEntityFieldByName(entity, column);
		return new WhereComparators(`"${column}"`, columnOptions.type);
	}

	return Object.assign(openWhere, whereUtils);
}

export function createJoinWhereBuilder<Shapes extends Record<string, object>>(
	knownEntities:
		| Map<string & keyof Shapes, EntityFromShape<unknown>>
		| Record<string & keyof Shapes, EntityFromShape<unknown>>,
) {
	const targetEntities =
		knownEntities instanceof Map
			? knownEntities
			: new Map(Object.entries(knownEntities));

	function openWhere<Alias extends string & keyof Shapes>(
		entityAlias: Alias,
		column: JsonRef<Shapes[Alias]>,
	): WhereComparators<unknown>;
	function openWhere<
		Alias extends string & keyof Shapes,
		Key extends string & keyof Shapes[Alias],
	>(entityAlias: Alias, column: Key): WhereComparators<Shapes[Alias][Key]>;
	function openWhere<
		Alias extends string & keyof Shapes,
		Key extends string & keyof Shapes[Alias],
	>(entityAlias: Alias, column: Key | JsonRef<Shapes[Alias]>) {
		const entity = targetEntities.get(entityAlias);
		if (!entity) {
			throw new Error(`Unrecognized entity alias '${entityAlias}'`);
		}

		if (isJsonRef(column)) {
			return new WhereComparators(
				`"${entityAlias}".${readJsonRef(column)}`,
				"jsonb",
			);
		}

		const columnOptions = getEntityFieldByName(entity, column);
		return new WhereComparators(
			`"${entityAlias}"."${column}"`,
			columnOptions.type,
		);
	}

	return Object.assign(openWhere, whereUtils);
}

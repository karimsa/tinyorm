import { sql, finalizeQuery, joinQueries, Entity } from "..";
import { describe, it } from "@jest/globals";
import { expectQuery } from "./util";
import { Column, isEntity } from "../entity";
import { readJsonRef } from "../queries";

describe("sql", () => {
	it("should prepare query statements with interpolated values (without casts)", () => {
		class foo extends Entity({ schema: "public", tableName: "foo" }) {}

		expect(isEntity(foo)).toEqual(true);
		expectQuery(
			finalizeQuery(sql`
				SELECT *
				FROM ${foo}
				WHERE created_at >= ${new Date("2022-01-01T20:47:18.789Z")}
					AND name = ${"test"}
					AND is_row = ${true}
					AND stuff = ${undefined}
					AND ${sql`gah @> ${{ bar: true }}`}
			`),
		).toEqual({
			text: `
				SELECT *
				FROM "public"."foo"
				WHERE created_at >= $1::timestamp
				  AND name = $2::text
				  AND is_row = $3::boolean
				  AND stuff = $4
				  AND gah @> $5::jsonb`,
			values: ["2022-01-01T20:47:18.789Z", "test", true, null, `{"bar":true}`],
		});
	});
	it("should join prepared statements with multiple variables", () => {
		expectQuery(
			finalizeQuery(
				joinQueries(
					sql`
						SELECT *
						FROM ${sql.getEntityRef({ schema: "public", tableName: "foo" })}
						WHERE created_at >= ${new Date("2022-01-01T20:47:18.789Z")}
						AND name = ${"test"}
					`,
					sql`
						AND is_row = ${true}
						AND stuff = ${undefined}
					`,
				),
			),
		).toEqual({
			text: `SELECT * FROM "public"."foo" WHERE created_at >= $1::timestamp AND name = $2::text AND is_row = $3::boolean AND stuff = $4`,
			values: ["2022-01-01T20:47:18.789Z", "test", true, null],
		});
	});
	it("should prepare query statements with interpolated values (with casts)", () => {
		expectQuery(
			finalizeQuery(sql`
				SELECT *
				FROM foo
				WHERE created_at >= ${sql.asText(true)}
			`),
		).toEqual({
			text: "SELECT * FROM foo WHERE created_at >= $1::text",
			values: ["true"],
		});
		expectQuery(
			finalizeQuery(sql`
				SELECT *
				FROM foo
				WHERE created_at >= ${sql.asBool(true)}
			`),
		).toEqual({
			text: "SELECT * FROM foo WHERE created_at >= $1::boolean",
			values: [true],
		});
		expectQuery(
			finalizeQuery(sql`
				SELECT *
				FROM foo
				WHERE created_at >= ${sql.asTimestamp(
					new Date("2022-01-01T20:47:18.789Z"),
				)}
			`),
		).toEqual({
			text: "SELECT * FROM foo WHERE created_at >= $1::timestamp",
			values: ["2022-01-01T20:47:18.789Z"],
		});
		expectQuery(
			finalizeQuery(sql`
				SELECT *
				FROM foo
				WHERE created_at >= ${sql.asDate(new Date("2022-01-01T20:47:18.789Z"))}
			`),
		).toEqual({
			text: "SELECT * FROM foo WHERE created_at >= $1::date",
			values: ["2022-01-01"],
		});
	});
	it("should allow unescaping parameters", async () => {
		expectQuery(
			finalizeQuery(sql`
				SELECT *
				FROM foo
				WHERE created_at >= ${sql.asUnescaped("true")}
			`),
		).toEqual({
			text: "SELECT * FROM foo WHERE created_at >= true",
			values: [],
		});
		expectQuery(
			finalizeQuery(sql`
				SELECT *
				FROM foo
				WHERE created_at >= ${sql.asUnescaped("'testing'")}
			`),
		).toEqual({
			text: "SELECT * FROM foo WHERE created_at >= 'testing'",
			values: [],
		});
		expectQuery(
			finalizeQuery(sql`
				SELECT *
				FROM foo
				WHERE created_at >= ${sql.asUnescaped(
					`'${new Date("2022-01-01T20:47:18.789Z").toISOString()}'`,
				)}
			`),
		).toEqual({
			text: "SELECT * FROM foo WHERE created_at >= '2022-01-01T20:47:18.789Z'",
			values: [],
		});
	});
	it("should produce json paths", () => {
		class TestClass extends Entity({ schema: "public", tableName: "test" }) {
			@Column({ type: "text" })
			readonly id: string;
			@Column({ type: "jsonb" })
			readonly nested_data: {
				readonly foo: string;
				readonly bar: {
					readonly baz: string;
					readonly biz: string[];
				}[];
			};
		}

		expect(readJsonRef(sql.json(TestClass).id)).toEqual(`"id"`);
		expect(readJsonRef(sql.json(TestClass).nested_data)).toEqual(
			`"nested_data"`,
		);
		expect(readJsonRef(sql.json(TestClass).nested_data.foo)).toEqual(
			`"nested_data"->"foo"`,
		);
		expect(readJsonRef(sql.json(TestClass).nested_data.bar)).toEqual(
			`"nested_data"->"bar"`,
		);
		expect(readJsonRef(sql.json(TestClass).nested_data.bar[0])).toEqual(
			`"nested_data"->"bar"->0`,
		);
		expect(readJsonRef(sql.json(TestClass).nested_data.bar[0].baz)).toEqual(
			`"nested_data"->"bar"->0->"baz"`,
		);
		expect(readJsonRef(sql.json(TestClass).nested_data.bar[0].biz)).toEqual(
			`"nested_data"->"bar"->0->"biz"`,
		);
		expect(readJsonRef(sql.json(TestClass).nested_data.bar[0].biz[5])).toEqual(
			`"nested_data"->"bar"->0->"biz"->5`,
		);
	});
});

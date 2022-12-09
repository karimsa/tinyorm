import { sql, finalizeQuery, joinQueries } from "..";
import { describe, it } from "@jest/globals";
import { expectQuery } from "./util";

describe("sql", () => {
	it("should prepare query statements with interpolated values (without casts)", () => {
		expectQuery(
			finalizeQuery(sql`
				SELECT *
				FROM foo
				WHERE created_at >= ${new Date("2022-01-01T20:47:18.789Z")}
				AND name = ${"test"}
				AND is_row = ${true}
				AND stuff = ${undefined}
			`),
		).toEqual({
			text: "SELECT * FROM foo WHERE created_at >= $1::timestamp AND name = $2::text AND is_row = $3::boolean AND stuff = $4",
			values: ["2022-01-01T20:47:18.789Z", "test", true, null],
		});
	});
	it("should join prepared statements with multiple variables", () => {
		expectQuery(
			finalizeQuery(
				joinQueries(
					sql`
						SELECT *
						FROM foo
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
			text: "SELECT * FROM foo WHERE created_at >= $1::timestamp AND name = $2::text AND is_row = $3::boolean AND stuff = $4",
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
});

# `@karimsa/tinyorm`

A tiny type-safe ORM for Postgres written in TypeScript.

**Table of Contents**

- [Entities](#entities)
- [Queries](#queries)
    - [Select Queries](#select-queries)
    - [Joins and More Complex Queries](#joins-and-more-complex-queries)
- [Migrations](#migrations)

## Entities

Entities are classes that represent a table in the database. They are defined by extending the `Entity` class and adding properties that represent the columns in the table.

```ts
import { Entity, Column, Index } from '@karimsa/tinyorm';

@Index(User)('idx_users_name', ['name'])
export class User extends Entity({ schema: 'app', tableName: 'users' }) {
  @Column({ type: 'uuid' })
  readonly id: string;

  @Column({ type: 'text' })
  readonly name: string;
}
```

## Queries

### Select Queries

```ts
import { createSelectBuilder } from '@karimsa/tinyorm';

// This will execute: SELECT * FROM app.users WHERE name = 'Karim';
// And the return type will be `User | null`.
const user = await createSelectBuilder()
  .from(User)
  .select(['id', 'name'])
  .where(where => where('name').Equals('Karim'))
  .getOne();
```

### Joins and More Complex Queries

```ts
import { createJoinBuilder, sql } from '@karimsa/tinyorm';

// This will execute something like:
//
// SELECT user.id, user.name FROM app.users AS user
// INNER JOIN app.user_posts AS user_post ON user.id = user_post.user_id
// WHERE user.name = 'Karim';
//
// And the return type will be `{ user: { id: string }; user_post: { id: string; title: string } }[]`.
//
const results = await createJoinBuilder()
  .from(User, 'user')
  .innerJoin(UserPost, 'user_post', sql`user.id = user_post.user_id`)
  .select('user', ['id'])
  .select('user_post', ['id', 'title'])
  .where(where => where('user', 'name').Equals('Karim'))
  .getMany();
```

### JSONB Queries

```ts
import { createJoinBuilder, sql } from '@karimsa/tinyorm';

const results = await createJoinBuilder()
  .from(User, 'user')
  .innerJoin(UserPost, 'user_post', sql`user.id = user_post.user_id`)
  .select('user', ['id'])
  .select('user_post', ['id', 'title'])
  // Calling `sql.json()` on any entity returns a `JsonBuilder` that can be used to query JSONB columns.
  // The types are derived from the shape of the entity, and the runtime value is transformed internally
  // into the correct JSONB query.
  .where(where => where('user', sql.json(User).nested.json.path).CastAs('text').Equals('foobar'))
  .getMany();
```

## Migrations

Calling `getMigrationQueries` on a `Connection` will return a set of `SuggestedMigration` objects that can be used to generate a migration file. The suggestions are generated by comparing the state of the table within the database to the state of the entity.

```ts
import { createConnectionPool } from '@karimsa/tinyorm';

const pool = await createConnectionPool({});

class User extends Entity({ schema: 'app', tableName: 'users' }) {
  @Column({ type: 'uuid' })
  readonly id: string;

  @Column({ type: 'text' })
  readonly name: string;
}

const suggestions = await pool.getMigrationQueries(User);

// This might generate something like the following for creating the table:
// [{
//      reason: 'New Table',
//      queries: [
//          'CREATE TABLE app.users (id uuid NOT NULL, name text NOT NULL);',
//      ],
// }]
```

To run a set of queries as a 'migration', simply call `executeMigration` on the `Connection` object:

```ts
await pool.withTransaction(async (tx) => {
  await tx.executeMigration('my migration', suggestions.map(s => s.queries).flat());
});
```

## License

Copyright (c) 2022-present Karim Alibhai.

Licensed under [MIT license](LICENSE.md).
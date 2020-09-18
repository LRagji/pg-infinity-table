# pg-infinity-table

This package combines redis cache and multiple postgres database together to form a large table spanning across DBs, and provides with following features
1. Row count partitioning.

2. Search with inbuilt time ranges.
3. Self Id generation or User provided Ids for primary key(user provided id limited to 20000000).
4. Custom schema defination.
6. Custom indexes.
7. Normal CRUD operations.

## Getting Started

1. Install using `npm i pg-infinity-table`
2. Require in your project. `const infinityType = require('pg-infinity-table');`
3. Run postgres as local docker if required. `docker run --name pg-12.4 -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=pgpartition -p 5432:5432 -d postgres:12.4-alpine`
4. Run redis as local docker if required. `docker run -p 6379:6379 -itd --rm redis:latest`
5. Create 2 worker database for table across DB Eg: Infinity-1 & Infinity-2
6. Instantiate with a postgres readers and writers and connections to redis. 
7. All done, Start using it!!.

## Examples/Code snippets

```javascript
const targetType = require('partition-pg').default;
const pgp = require('pg-promise')();
const defaultConectionString = "postgres://postgres:mysecretpassword@localhost:5432/pgpartition?application_name=perf-test";
const tableName = "Raw", schemaName = "Anukram";
const readConfigParams = {
    connectionString: defaultConectionString,
    application_name: "e2e Test",
    max: 4 //4 readers
};
const writeConfigParams = {
    connectionString: defaultConectionString,
    application_name: "e2e Test",
    max: 2 //2 Writer
};
const _dbRConnection = pgp(readConfigParams);
const _dbWConnection = pgp(writeConfigParams);
const tableSchema = [
{
    "name": "time",
    "datatype": "bigint",
    "filterable": { "sorted": "desc" },
    "primary": true,
    "key": {
        "range": 1000
    }
},
{
    "name": "tagid",
    "datatype": "integer",
    "primary": true
},
{
    "name": "value",
    "datatype": "double",
},
{
    "name": "quality",
    "datatype": "integer",
    "filterable": { "sorted": "asc" },
}];
 const partitionManager = new targetType(_dbRConnection, _dbWConnection, schemaName, tableName,tableSchema);

//Create the table definition in database.
  await partitionManager.create();

//Insert or Update data based on primary key.
let insertpayload = [
    [0, 1, 1.5, 1],
    [999, 2, 2.5, 2],
]

await partitionManager.upsert(insertpayload);

//Read data by range.
let result = await partitionManager.readRange(0, 998);

```

## Built with

1. Authors love for Open Source.
2. [pg-promise](https://www.npmjs.com/package/pg-promise).
3. [ioredis](https://www.npmjs.com/package/ioredis).
4. [redis-scripto](https://www.npmjs.com/package/redis-scripto).

## Contributions

1. New ideas/techniques are welcomed.
2. Raise a Pull Request.

## Current Version:
0.0.1[Beta]

## License

This project is contrubution to public domain and completely free for use, view [LICENSE.md](/license.md) file for details.
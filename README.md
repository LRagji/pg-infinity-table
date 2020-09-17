# pg-infinity-table

This package combines redis cache and multiple postgres database together to form a large table spanning across DBs, and provides with following features
1. App level table partitioning without any specific pg implementation.
2. Partition key can be any column but has to be monotonic integer.
3. Efficient extraction of data.
4. Custom schema defination.
5. Custom primary key.
6. Custom indexes.
7. Insert/Update in a single call.

## Getting Started

1. Install using `npm -i partition-pg`
2. Require in your project. `const targetType = require('partition-pg').PartionPg;`
3. Run postgres on local docker if required. `docker run --name pg-12.4 -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=pgpartition -p 5432:5432 -d postgres:12.4-alpine`
3. Instantiate with a postgres connection with readers and writers. 
4. All done, Start using it!!.

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
3. [ts-map](https://www.npmjs.com/package/ts-map?activeTab=readme).

## Contributions

1. New ideas/techniques are welcomed.
2. Raise a Pull Request.

## Current Version:
0.0.1[Beta]

## License

This project is contrubution to public domain and completely free for use, view [LICENSE.md](/license.md) file for details.
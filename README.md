# pg-infinity-table

This package combines redis cache and multiple postgres database together to contain a large(infinite) tables spanning across DBs,with following features
1. Row count partitioning.
2. Search with inbuilt time ranges.
3. Self Id generation or User provided Ids for primary key(user provided id limited to 20000000).
4. Custom schema defination.
6. Custom indexes.
7. Normal CRUD operations.
8. Paginated results for all calls.

## Getting Started

1. Install using `npm i pg-infinity-table`
2. Require in your project. `const infinityType = require('pg-infinity-table');`
3. Run postgres as local docker if required. `docker run --name pg-12.4 -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=pg-infinity-meta -p 5432:5432 -d postgres:12.4-alpine`
4. Run redis as local docker if required. `docker run -p 6379:6379 -itd --rm redis:latest`
5. Create 2 worker database for table across DB Eg: Infinity-1 & Infinity-2
6. Instantiate with a postgres readers and writers and connections to redis. 
7. All done, Start using it!!.

## Examples/Code snippets

1. **Initialize**
```javascript
const infinityTableType = require('pg-infinity-table');
const metaRedisConectionString = "redis://127.0.0.1:6379/";
const metaPGConectionString = "postgres://postgres:mysecretpassword@localhost:5432/pg-infinity-meta?application_name=perf-test";
const readConfigParams = {
    connectionString: metaPGConectionString,
    application_name: "Infinity Test",
    max: 4 //4 readers
};
const writeConfigParams = {
    connectionString: metaPGConectionString,
    application_name: "Infinity Test",
    max: 2 //2 Writer
};
const infinityDatabase = await infinityTableType(metaRedisConectionString, readConfigParams, writeConfigParams)
```

2. **Add Resources**
```javascript
const maxTablesPerResource = 1000; //Maximum Tables that will be used to create infinity tables.
const maxRowsPerTable = 100; //<-Row Partitioning
const readConfigParamsDB1 = {
    connectionString: "postgres://postgres:@localhost:5432/Infinity-1",
    application_name: "e2e Test",
    max: 4 //4 readers
};
const writeConfigParamsDB1 = {
    connectionString: "postgres://postgres:@localhost:5432/Infinity-1",
    application_name: "e2e Test",
    max: 2 //2 Writer
};
const readConfigParamsDB2 = {
    connectionString: "postgres://postgres:@localhost:5432/Infinity-2",
    application_name: "e2e Test",
    max: 4 //4 readers
};
const writeConfigParamsDB2 = {
    connectionString: "postgres://postgres:@localhost:5432/Infinity-2",
    application_name: "e2e Test",
    max: 2 //2 Writer
};
let resourceId = await infinityDatabase.registerResource(readConfigParamsDB1, writeConfigParamsDB1, maxTablesPerResource, maxRowsPerTable);
console.log("Resource Id:" + resourceId);

resourceId = await infinityDatabase.registerResource(readConfigParamsDB2, writeConfigParamsDB2, maxTablesPerResource, maxRowsPerTable);
console.log("Resource Id:" + resourceId);
```

3. **Create Infinity Table**
```javascript
const TableDefinition = [{
    "name": "time",
    "datatype": "bigint"
},
{
    "name": "tagid",
    "datatype": "integer"
},
{
    "name": "value",
    "datatype": "double",
},
{
    "name": "quality",
    "datatype": "integer"
}];

const boundlessTable = await infinityDatabase.createTable(TableDefinition);
const tableId = boundlessTable.TableIdentifier;
console.log("Table Created: " + tableId);
```
4. **Insert Data**
```javascript
let ctr = 200;
let rows = [];
while (ctr > 0) {
    rows.push({ "time": ctr, "tagid": ctr, "value": ctr, "quality": ctr })
    ctr--;
}

console.time("Insertion");
const result = await boundlessTable.insert(rows);
console.timeEnd("Insertion");
```
5. **Search Data**
```javascript
const filter = {
    "conditions": [
        {
            "name": "value",
            "operator": "=",
            "values": [
                10
            ]
        },
        {
            "name": "quality",
            "operator": "=",
            "values": [
                1
            ]
        }
    ],
    "combine": "$1:raw OR $2:raw"
}
const results = await boundlessTable.search(undefined, undefined, filter);
console.log(results);
```

6. **Load Existing Table**
```javascript
const boundlessTable = await infinityDatabase.loadTable(TypeId);
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

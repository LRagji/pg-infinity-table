//const defaultConectionString = "postgres://postgres:mysecretpassword@localhost:5432/pgpartition?application_name=perf-test";
const defaultConectionString = "postgres://postgres:@localhost:5432/Infinity-Index";
const defaultRedisConnectionString = "redis://127.0.0.1:6379/";
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
let TableWithoutUserId = [{
    "name": "time",
    "datatype": "bigint",
    "filterable": { "sorted": "desc" }
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
    "datatype": "integer",
    "filterable": { "sorted": "asc" },
}];
let TableWithUserId = [{
    "name": "time",
    "datatype": "bigint",
    "filterable": { "sorted": "desc" },
    "tag": "primary"
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
    "datatype": "integer",
    "filterable": { "sorted": "asc" },
}];
const infTableType = require('./index');

const infTableFactory = new infTableType(defaultRedisConnectionString, readConfigParams, writeConfigParams)
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
let TypeId = 2;
let boundlessTable;
let main = async () => {
    if (TypeId == undefined) {
        // let resourceId = await infTableFactory.registerResource(readConfigParamsDB1, writeConfigParamsDB1, 1000, 100);
        // console.log("Resource Id:" + resourceId);
        // resourceId = await infTableFactory.registerResource(readConfigParamsDB2, writeConfigParamsDB2, 1000, 100);
        // console.log("Resource Id:" + resourceId);
        boundlessTable = await infTableFactory.createTable(TableWithoutUserId);
        TypeId = boundlessTable.TableIdentifier;
        console.log("Type Created: " + TypeId);
        return;
    }
    else {
        boundlessTable = await infTableFactory.loadTable(TypeId);
    }

    // let ctr = 200;
    // let payload = [];
    // console.time("Payload Generation");
    // while (ctr > 0) {
    //     payload.push({ "time": ctr, "tagid": ctr, "value": ctr, "quality": ctr })
    //     ctr--;
    // }
    // console.timeEnd("Payload Generation");

    // console.time("Insertion");
    // let result = await boundlessTable.bulkInsert(payload);
    // console.timeEnd("Insertion");

    // if (result.failures.length > 0) {
    //     console.error("Failed:" + result.failures[0]);
    // }
    // else {
    //     console.log("Sucess:" + result.success.length);
    // }
    let results ;
    // console.time("Retrive");
    // results = await boundlessTable.retrive(["2-1-997-1000"]);
    // console.timeEnd("Retrive");
    // console.log(results);

    // console.time("CompleteTypeSearch");
    // let filter = {
    //     "conditions": [
    //         {
    //             "name": "value",
    //             "operator": "=",
    //             "values": [
    //                 10
    //             ]
    //         },
    //         {
    //             "name": "quality",
    //             "operator": "=",
    //             "values": [
    //                 1
    //             ]
    //         }
    //     ],
    //     "combine": "$1:raw OR $2:raw"
    // }
    // results = await boundlessTable.search(undefined, undefined, filter);
    // console.timeEnd("CompleteTypeSearch");
    // console.log(results);

    // console.time("RangeTypeSearch");
    // filter = {
    //     "conditions": [
    //         {
    //             "name": "value",
    //             "operator": "=",
    //             "values": [
    //                 10
    //             ]
    //         },
    //         {
    //             "name": "quality",
    //             "operator": "=",
    //             "values": [
    //                 1
    //             ]
    //         }
    //     ],
    //     "combine": "$1:raw OR $2:raw"
    // }
    // results = await boundlessTable.search(10, 40, filter);
    // console.timeEnd("RangeTypeSearch");
    // console.log(results);

    // console.time("NoFilterRangeSearch");
    // results = await boundlessTable.search(30, 60);
    // console.timeEnd("NoFilterRangeSearch");
    // console.log(results);

    // console.time("PagedSearch");
    // results = await boundlessTable.search(undefined, undefined, undefined, ["2-1-995"]);
    // console.timeEnd("PagedSearch");
    // console.log(results);

    boundlessTable.codeRed();
};

main().then((r) => {
    infTableFactory.codeRed();
});


// CREATE TABLE public."Resources"
// (
//     "Id" bigserial,
//     "Read" text NOT NULL,
//     "Write" text NOT NULL,
//     "MaxTables" integer NOT NULL,
//     "MaxRows" integer NOT NULL,
//     PRIMARY KEY ("Id")
// );

// CREATE TABLE public."Types"
// (
//     "Id" bigserial,
//     "Def" text[] NOT NULL,
//     PRIMARY KEY ("Id")
// );

//SQL for Reindexing
// SELECT split_part(tablename,'-',1) as "TypeId",split_part(tablename,'-',2) as "DBId",split_part(tablename,'-',3)as "TableId",
// tablename, n_live_tup as "ERows"
// FROM pg_catalog.pg_tables as "T" JOIN pg_stat_user_tables as "S" on "T".tablename="S".relname
// where tablename like '13-%' and n_live_tup !=100

//40K 10K/Sec
// Payload Generation: 6.198ms
// Acquiring Identity: 62.97509765625ms
// Transforming: 48.083ms
// Inserting: 3284.655ms

//150K
//Payload Generation: 31.56494140625ms
// Identity: 213.784ms
// Transform: 314.534ms
// PG: 14188.743896484375ms
// Insertion: 14717.776ms

//150000
// Payload Generation: 27.8369140625ms
// Identity: 214.527ms
// Transform: 345.728ms
// PG: 16912.221ms
// Insertion: 17473.642ms
// Sucess:1500
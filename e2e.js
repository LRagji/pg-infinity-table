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
},
{
    "name": "sometext",
    "datatype": "text"
}];
const infinityTable = require('./index');

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
let TypeId = 1;
let boundlessTable;
let infinityDatabase;
let main = async () => {
    infinityDatabase = await infinityTable(defaultRedisConnectionString, readConfigParams, writeConfigParams)

    if (TypeId == undefined) {
        let resourceId = await infinityDatabase.registerResource(readConfigParamsDB1, writeConfigParamsDB1, 1000, 10000000);
        console.log("Resource Id:" + resourceId);
        resourceId = await infinityDatabase.registerResource(readConfigParamsDB2, writeConfigParamsDB2, 1000, 10000000);
        console.log("Resource Id:" + resourceId);
        boundlessTable = await infinityDatabase.createTable(TableWithUserId);
        TypeId = boundlessTable.TableIdentifier;
        console.log("Type Created: " + TypeId);
        return;
    }
    else {
        boundlessTable = await infinityDatabase.loadTable(TypeId);
    }
    // let lctr = 100
    // while (lctr < 101) {
    //     let offset = lctr * 100000;
    //     let ctr = offset + 100000;
    //     let payload = [];
    //     console.time("Payload Generation");
    //     while (ctr > offset) {
    //         payload.push({ "time": ctr, "tagid": getRandomInt(100, 100), "value": getRandomInt(1, 10000), "quality": getRandomInt(ctr, ctr + 1000), "sometext": makeString(100) })
    //         ctr--;
    //     }
    //     console.timeEnd("Payload Generation");

    //     console.time("Insertion");
    //     let result = await boundlessTable.bulkInsert(payload);
    //     console.timeEnd("Insertion");
    //     console.log(result);
    //     lctr++;
    // }
    // if (result.failures.length > 0) {
    //     console.error("Failed:" + result.failures[0]);
    // }
    // else {
    //     console.log("Sucess:" + result.success.length);
    // }
    let results;
    // console.time("Retrive");
    // results = await boundlessTable.retrive([83928n]);
    // console.timeEnd("Retrive");
    // console.log(results);

    console.time("CompleteTypeSearch");
    let filter = {
        "conditions": [
            {
                "name": "value",
                "operator": "=",
                "values": [
                    568
                ]
            },
            {
                "name": "quality",
                "operator": "=",
                "values": [
                    0
                ]
            },
            {
                "name": "sometext",
                "operator": "=",
                "values": [
                    'kfldkdl'
                ]
            }

        ],
        "combine": "$1:raw OR $2:raw"
    }
    results = await boundlessTable.search(undefined, undefined, filter);
    console.timeEnd("CompleteTypeSearch");
    console.log(results.pages);
    //console.table(results.results);

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
};

main().then((r) => {
    console.log("Done");
});

function makeString(length) {
    var result = '';
    var characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var charactersLength = characters.length;
    for (var i = 0; i < length; i++) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
}

function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min) + min); //The maximum is exclusive and the minimum is inclusive
}
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
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
},
{
    "name": "sometext",
    "datatype": "text"
}
];
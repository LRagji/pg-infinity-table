const defaultConectionString = "postgres://postgres:postgres@10.23.69.72:5432/Infinity-Index";
const shard1 = "postgres://postgres:postgres@10.23.69.72:5432/Infinity-Shard-1";
const shard2 = "postgres://postgres:postgres@10.23.69.72:5432/Infinity-Shard-2";
const defaultRedisConnectionString = "redis://10.23.69.72:6379/";
const AppName = "LaukikTest";
const infinityTable = require('./index');
const { v4: uuidv4 } = require('uuid');
const readConfigParams = {
    connectionString: defaultConectionString,
    application_name: AppName,
    max: 4 //4 readers
};
const writeConfigParams = {
    connectionString: defaultConectionString,
    application_name: AppName,
    max: 2 //2 Writer
};
const readConfigParamsShard1 = {
    connectionString: shard1,
    application_name: AppName,
    max: 4 //4 readers
};
const writeConfigParamsShard1 = {
    connectionString: shard1,
    application_name: AppName,
    max: 2 //2 Writer
};
const readConfigParamsShard2 = {
    connectionString: shard2,
    application_name: AppName,
    max: 4 //4 readers
};
const writeConfigParamsShard2 = {
    connectionString: shard2,
    application_name: AppName,
    max: 2 //2 Writer
};
let AlertType = [
    {
        "name": "EventAckDHPId",
        "datatype": "text"
    },
    {
        "name": "EventID",
        "datatype": "text",
        "filterable": { "sorted": "desc" },
        "tag": "primary"
    },
    {
        "name": "NodeID",
        "datatype": "text",
    },
    {
        "name": "EnteredDateTime",
        "datatype": "bigint",
        "tag": "InfStamp"
    },
    {
        "name": "EventType",
        "datatype": "text"
    },
    {
        "name": "Severity",
        "datatype": "integer"
    },
    {
        "name": "Machine",
        "datatype": "text"
    },
    {
        "name": "Measurement",
        "datatype": "text"
    },
    {
        "name": "Point",
        "datatype": "text"
    },
    {
        "name": "LeftDateTime",
        "datatype": "text"
    },
    {
        "name": "Setpoint1",
        "datatype": "double"
    },
    {
        "name": "Setpoint2",
        "datatype": "double"
    },
    {
        "name": "IsActive",
        "datatype": "integer"
    },
    {
        "name": "AckedBy",
        "datatype": "text"
    },
    {
        "name": "AckedDateTime",
        "datatype": "text"
    },
    {
        "name": "Instrumentation",
        "datatype": "text"
    },
    {
        "name": "PointID",
        "datatype": "text"
    },
    {
        "name": "StatusBit",
        "datatype": "integer"
    },
    {
        "name": "TriggerValue",
        "datatype": "double"
    },
    {
        "name": "Mode",
        "datatype": "integer"
    },
    {
        "name": "Model",
        "datatype": "text"
    },
    {
        "name": "Speed",
        "datatype": "double"
    },
    {
        "name": "DeviceEventID",
        "datatype": "integer"
    },
    {
        "name": "DeviceID",
        "datatype": "text"
    },
    {
        "name": "ModeName",
        "datatype": "text"
    },
    {
        "name": "ServerTimeStamp",
        "datatype": "text"
    },
    {
        "name": "WrittenTimestamp",
        "datatype": "text"
    },
    {
        "name": "SourceMachineName",
        "datatype": "text"
    }
];
entryPoint = async () => {
    const infinityDatabase = await infinityTable(defaultRedisConnectionString, readConfigParams, writeConfigParams);

    if (process.argv[2].toLowerCase() === "register") {
        const tables = 1000, rowsPerTable = 10000000;
        let resourceId = await infinityDatabase.registerResource(readConfigParamsShard1, writeConfigParamsShard1, tables, rowsPerTable);
        console.log(`Shard-1 registered with id ${resourceId}`);
        resourceId = await infinityDatabase.registerResource(readConfigParamsShard2, writeConfigParamsShard2, tables, rowsPerTable);
        console.log(`Shard-2 registered with id ${resourceId}`);
        const boundlessTable = await infinityDatabase.createTable(AlertType);
        console.log(`Type registered with id ${boundlessTable.TableIdentifier}`);
    }
    else if (process.argv[2].toLowerCase() === "fill") {
        const typeId = parseInt(process.argv[3]);
        const batches = parseInt(process.argv[4]);
        const boundlessTable = await infinityDatabase.loadTable(typeId);
        console.time("Fill");
        await fill(boundlessTable, batches);
        console.timeEnd("Fill");

    }
    else {
        console.log("Unknown argument:" + process.argv[2]);
    }
}

async function fill(boundlessTable, batches) {
    //INSERTS
    let batchCounter = 0
    const batchSize = 10000;
    while (batchCounter < batches) {
        let batch = [];
        console.time("Payload Generation");
        let alertCounter = 0;
        while (alertCounter < 10000) {
            batch.push(generateAlert(alertCounter, batchCounter));
            alertCounter++;
        }
        console.timeEnd("Payload Generation");

        console.time("Insertion");
        let result = await boundlessTable.insert(batch);
        console.timeEnd("Insertion");

        console.log(`Success: ${Math.floor((result.success[0].length / batchSize) * 100)}%, Failures: ${Math.floor((result.failures.length / batchSize) * 100)}%`);
        console.log("");
        batchCounter++;
    }
}

function generateAlert(batchCounter, batchNumber) {
    return {
        "EventID": uuidv4(),
        "EventAckDHPId": uuidv4(),
        "NodeID": uuidv4(),
        "EnteredDateTime": Date.now() + batchCounter + batchNumber,
        "EventType": makeString(10),
        "Severity": getRandomInt(0, 5),
        "Machine": makeString(60),
        "Measurement": makeString(70),
        "Point": getRandomInt(0, batchCounter),
        "LeftDateTime": makeString(20),
        "Setpoint1": getRandomInt(0, batchCounter) + 0.06,
        "Setpoint2": getRandomInt(0, batchCounter) + 0.03,
        "IsActive": getRandomInt(0, 1),
        "AckedBy": makeString(15),
        "AckedDateTime": makeString(20),
        "Instrumentation": makeString(20),
        "PointID": makeString(10),
        "StatusBit": getRandomInt(0, batchCounter),
        "TriggerValue": getRandomInt(0, batchCounter) + 0.009,
        "Mode": getRandomInt(0, batchCounter),
        "Model": makeString(10),
        "Speed": getRandomInt(0, batchCounter) + 0.04,
        "DeviceEventID": getRandomInt(0, batchCounter),
        "DeviceID": uuidv4(),
        "ModeName": makeString(10),
        "ServerTimeStamp": new Date().toString(),
        "WrittenTimestamp": new Date().toString(),
        "SourceMachineName": makeString(10)
    };
}

console.time("Program completed");
entryPoint().then((r) => {
    console.timeEnd("Program completed");
}).catch((err) => {
    console.error(err);
    console.timeEnd("Program completed");
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
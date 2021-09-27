const mariadb = require('mariadb');
const TeradataConnection = require("teradata-nodejs-driver/teradata-connection");
const fs = require("fs").promises;

const { getSystemErrorMap } = require('util');
// initialized

var options = {
    source1: {

    },
    source2: {

    },
    source3: {

    },
    TeradataDbConnectionConfigurations: {
        type: "teradata",
        log: 0,
        host: 'ripsaw.skechers.com',
        user: 'expadmin',
        password: 'scbltE!',
        acquireTimeout: 10000,
        connectionLimit: 5
    },
    mariaDbConnectionConfigurations: {
        type: "mariadb",
        host: "talend-mariadb-prod.chlnajphzxst.us-west-2.rds.amazonaws.com",
        user: "svcdeprd",
        password: "jSwAZi2Jjcra4VsPgmoLBMCypu3c8EtyoN9wKbyOg",
        connectionLimit: 5
    },
    snowflakeDbConnectionConfigurations: {

    },
    DeltaLakeBronzeDbConnectionConfigurations: {

    },
    AproPOSDbConnectionConfigurations: {

    },
    CompassDbConnectionConfiguration: {

    }
}

let workerFunctions = {
    welcome: function(inputOptions, dbName) {
        // console.log('', inputOptions);

        //need custom logic, when more than 1
        var outputOptions = {
            source1: options.mariaDbConnectionConfigurations,
            source2: options.TeradataDbConnectionConfigurations
        }

        let outputConnectionPool;

        // console.log("dbName:", dbName);
        switch (dbName) {
            case "mariaDb":
                outputConnectionPool = mariadb.createPool(outputOptions.source1);
                break;
            case "teradataDb":
                outputConnectionPool = new TeradataConnection.TeradataConnection();
                // teradataConnection.connect(connParams);
                // console.log("Connect Success");
                // teradataConnection.close();
                break;
        }

        // console.log("outputConnectionPool:", outputConnectionPool);
        return outputConnectionPool;
    },
    solveTheProblem() {
        console.log('주영아, 사랑해~~~');
        console.log('args:', args);
        return "problem not solved yet";
    },
    shareTheSolution() {
        console.log('서은이, 사랑해~~~');
        console.log('solutionContent:', args);
        return "no solution content yet";
    },
    cleanUp() {
        console.log('자, 그럼 어떤 결과가 있었는지 함 볼까?');
        console.log('finalSteps:', args);
        return "no result to see yet";
    }
};

//console.log("\nworkerFunctions:", workerFunctions);


const pool = workerFunctions.welcome(options.mariaDbConnectionConfigurations, "mariaDb");

// const teradataConnection = workerFunctions.welcome(options.TeradataDbConnectionConfigurations, "teradataDb");

console.log('setup complete');

async function asyncFunction() {
    let connMariaDb;
    try {
        //read the file
        let table_names = (await fs.readFile("./resources/table_list_DE877.csv", "utf-8")).split(/\r?\n/).slice(1);
        console.log('table_names:', table_names);

        // connMariaDb = await pool.getConnection();
        // connTeradataDb = await teradataConnection.connect(connParams);

        // let mariaDbSchemaQueryTables = `SELECT table_schema as \`DB\`, table_name AS \`Table\`, 
        // ROUND(((data_length + index_length) / 1024 / 1024), 2) \`Size (MB)\` 
        // FROM information_schema.TABLES 
        // ORDER BY (data_length + index_length) DESC; 
        // `;

        // const rows = await connMariaDb.query(mariaDbSchemaQueryTables);
        // console.log(rows); //[ {val: 1}, meta: ... ]
        //const res = await conn.query("INSERT INTO myTable value (?, ?)", [1, "mariadb"]);
        //console.log(res); // { affectedRows: 1, insertId: 1, warningStatus: 0 }

    } catch (err) {
        throw err;
    } finally {
        console.log('done, cleanup');
        if (connMariaDb) return connMariaDb.end();
        // if (teradataConnection) return teradataConnection.close();
    }
}

asyncFunction();
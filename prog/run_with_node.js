const mariadb = require('mariadb');
const TeradataConnection = require("teradata-nodejs-driver/teradata-connection");
const fs = require("fs").promises;
const _ = require("underscore");

const { getSystemErrorMap } = require('util');
// initialized

var options = {
    source1: {

    },
    source2: {

    },
    source3: {

    },
    TeradataDbConnectionConfigurations: require("./teradata/"),
    mariaDbConnectionConfigurations: require('./mariadb/'),
    snowflakeDbConnectionConfigurations: {

    },
    // DeltaLakeBronzeDbConnectionConfigurations: {

    // }
    // ,
    // AproPOSDbConnectionConfigurations: {

    // },
    // CompassDbConnectionConfiguration: {

    // }
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
                outputConnectionPool.connect(outputOptions.source2);
                console.log("Connect Success");
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


// const pool = workerFunctions.welcome(options.mariaDbConnectionConfigurations, "mariaDb");

// const teradataConnection = workerFunctions.welcome(options.TeradataDbConnectionConfigurations, "teradataDb");

console.log('setup complete');

async function asyncFunction() {
    let connMariaDb, connTeradataDb;
    try {
        //read the file
        let table_names = (await fs.readFile("./resources/table_list_DE877.csv", "utf-8")).split(/\r?\n/).slice(1);
        // console.log('table_names:', table_names);

        // connMariaDb = await pool.getConnection();

        // let mariaDbSchemaQueryTables = `SELECT table_schema as \`DB\`, table_name AS \`Table\`, 
        // ROUND(((data_length + index_length) / 1024 / 1024), 2) \`Size (MB)\` 
        // FROM information_schema.TABLES 
        // ORDER BY (data_length + index_length) DESC; 
        // `;

        // const rows = await connMariaDb.query(mariaDbSchemaQueryTables);

        // console.log("mariaDb RESULTS:\n\n-----------\n", rows); //[ {val: 1}, meta: ... ]
        //const res = await conn.query("INSERT INTO myTable value (?, ?)", [1, "mariadb"]);
        //console.log(res); // { affectedRows: 1, insertId: 1, warningStatus: 0 }

        // let tableSQL = _.forEach(table_names, function(item) {
        //     return "'" + item + "',";
        // });
        let tableSQL = "";
        for (let i = 0; i < table_names.length; i++) {
            tableSQL += "'" + table_names[i] + "'";
            if (i + 1 < table_names.length) {
                tableSQL += ",";
            }
        }

        let teradataDbSchemaQueryTables = "SELECT DatabaseName, TableName, ColumnName, ColumnFormat FROM DBC.ColumnsV WHERE tableName IN (" + tableSQL + ")";

        let dataTypesToSample = ['char', 'number', 'date', 'timestamp_ntz'];

        // console.log('\n\n-----------\nquery including all tables from list:', teradataDbSchemaQueryTables);

        console.log('running query\n\n-----------\n');

        var teradataConnection = new TeradataConnection.TeradataConnection();
        teradataConnection.connect(options.TeradataDbConnectionConfigurations);
        var cursor = await teradataConnection.cursor();
        cursor.execute(teradataDbSchemaQueryTables);
        console.log('execute sent to TD cursor');
        const tdTablesMatchingTableList = cursor.fetchall();

        console.log('results:', tdTablesMatchingTableList.length);
        cursor.close();
        //console.log("teradata RESULTS:\n\n-----------\n", tdTablesMatchingTableList); //[ {val: 1}, meta: ... ]



    } catch (err) {
        console.log("err:", err);
        throw err;
    } finally {
        console.log('done, cleanup');
        if (connMariaDb) return connMariaDb.end();
        if (teradataConnection) return teradataConnection.close();
    }
}

asyncFunction();
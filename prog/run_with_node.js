const mariadb = require('mariadb');
const TeradataConnection = require("teradata-nodejs-driver/teradata-connection");
const SnowflakeSDK = require("snowflake-sdk");
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
        account: "SKECHERS",
        username: "svc_erwin",
        password: "0^ag5L6C$0W2V1x28tOm9"
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
            case "snowflakeDb":
                outputConnectionPool = new SnowflakeSDK.createConnection(inputOptions);
                break;
        }

        // console.log("outputConnectionPool:", outputConnectionPool);
        if (!outputConnectionPool) throw new Error('\n\n\nempty OutputConnectionPool - dbName:', dbName);

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




console.log('setup complete');

async function asyncMain() {
    let connMariaDb, connTeradataDb, connSnowflakeDb;
    try {
        //read the file
        let table_names = (await fs.readFile("./resources/table_list_DE877.csv", "utf-8")).split(/\r?\n/).slice(1);
        // console.log('table_names:', table_names);

        let tableSQL = "";
        for (let i = 0; i < table_names.length; i++) {
            tableSQL += "'" + table_names[i] + "'";
            if (i + 1 < table_names.length) {
                tableSQL += ",";
            }
        }

        let dataTypesToSample = [{
            dataType: 'char',
            teradata: [''],
            snowflake: ['']
        }, {
            dataType: 'number',
            teradata: [''],
            snowflake: ['']
        }, {
            dataType: 'date',
            teradata: [''],
            snowflake: ['']
        }, {
            dataType: '',
            teradata: [''],
            snowflake: ['']
        }];

        // MariaDB 
        /*
        // const pool = workerFunctions.welcome(options.mariaDbConnectionConfigurations, "mariaDb");
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
        */

        // const teradataConnection = workerFunctions.welcome(options.TeradataDbConnectionConfigurations, "teradataDb");

        connSnowflakeDb = workerFunctions.welcome(options.snowflakeDbConnectionConfigurations, "snowflakeDb");

        let snowflakeDbSchemaQueryString = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME IN (" + tableSQL + ");";
        // let teradataDbSchemaQueryTables = "SELECT DatabaseName, TableName, ColumnName, ColumnFormat FROM DBC.ColumnsV WHERE tableName IN (" + tableSQL + ")";

        // console.log('sf connection (pre-connect):', connSnowflakeDb.getId());

        // console.log('\n\n-----------\nquery including all tables from list:', teradataDbSchemaQueryTables);

        console.log('running query\n\n-----------\n');

        let executeResult = connSnowflakeDb.execute({
            sqlText: "select 1",
            complete: function(err, stmt, rows) {
                if (err) {
                    console.log('executionSF failure:', err);
                } else {
                    console.log('rowsSF.length:', rows ? rows.length : 'empty object');
                }
            }
        });

        // TERADATA (start)
        /*
        connTeradataDb = new TeradataConnection.TeradataConnection();
        connTeradataDb.connect(options.TeradataDbConnectionConfigurations);
        let cursor = await connTeradataDb.cursor();
        cursor.execute(teradataDbSchemaQueryTables);
        console.log('execute sent to TD cursor');
        const tdTablesMatchingTableList = cursor.fetchmany(9);

        console.log('results:', tdTablesMatchingTableList.length);
        if (tdTablesMatchingTableList.length < 10) {
            console.log('less than 10 results:\n');
            tdTablesMatchingTableList.forEach(function(item) {
                console.log('item:', item); // null returned for ColumnFormat in TD, use SF to drive column-selection logic, then identify equivalent in TD
            });
        }
        cursor.close();
        //console.log("teradata RESULTS:\n\n-----------\n", tdTablesMatchingTableList); //[ {val: 1}, meta: ... ]
        */
        // TERADATA end

        // SNOWFLAKE (start)
        connSnowflakeDb.connect(function(err, conn) {

            console.log('inside-SFconnect\nerr:', err);
            // console.log('conn:', conn);

            if (err) {
                console.log('Unable to connect:', err.message);
                throw err;
            } else {
                console.log('connected to SF');
                let connectionId = conn.getId();
                console.log('connectionId (post-connect):', connectionId);
            }
        });
        // SNOWFLAKE (end)


    } catch (err) {
        console.log("err:", err);
        throw err;
    } finally {
        console.log('done, cleanup initiating...');
        if (connMariaDb) return connMariaDb.end();
        if (connTeradataDb) return teradataConnection.close();
        if (connSnowflakeDb) return connSnowflakeDb.destroy(function(err, conn) {
            if (err) {
                console.log('unable to disconnect: ', err.message);
            }
        })
    }
}

asyncMain();
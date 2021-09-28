const { getSystemErrorMap } = require('util');

const mariadb = require('mariadb');
const fs = require("fs").promises;
const fs2 = require('fs');
const csv = require('csv-parser');

const loadSnowflakeData = require('./snowflake/');
const loadTeradataData = require('./teradata/');
const { Console } = require('console');

async function asyncMain() {

    try {
        //read the file
        let table_names = (await fs.readFile("./resources/table_list_DE877.csv", "utf-8")).split(/\r?\n/).slice(1);
        console.log('table_names:', table_names.length);

        let snowflakeColumnsFromTableNames, teradataColumnsFromTableNames;
        let csvSchema = [
            { id: 'TABLE_SCHEMA', title: 'ContainerName' },
            { id: 'TABLE_NAME', title: 'TableName' },
            { id: 'COLUMN_NAME', title: 'ColumnName' },
            { id: 'DATA_TYPE', title: 'ColumnType' }
        ];

        //check if SF file-cache exists locally
        let sfFileName = './sf_data.csv';
        try {
            if (fs2.existsSync(sfFileName)) {
                //use the file
                if (!snowflakeColumnsFromTableNames) {
                    snowflakeColumnsFromTableNames = [];
                }
                fs2.createReadStream(sfFileName)
                    .pipe(csv())
                    .on('data', (row) => {
                        snowflakeColumnsFromTableNames.push(row);
                    }).on('end', () => {
                        console.log(sfFileName, 'successfully read-in:', snowflakeColumnsFromTableNames.length);
                        console.log('sf record[0]:', snowflakeColumnsFromTableNames[0]);
                    })
            } else {
                //get data
                snowflakeColumnsFromTableNames = await loadSnowflakeData(table_names);
                console.log('SF column results:', snowflakeColumnsFromTableNames.length);
                console.log('sf record[0]:', snowflakeColumnsFromTableNames[0]);
                //write the file
                const writer = require('csv-writer').createObjectCsvWriter({
                        path: sfFileName,
                        header: csvSchema
                    }).writeRecords(snowflakeColumnsFromTableNames)
                    .then(() => console.log(sfFileName, console.log('SF column results:', snowflakeColumnsFromTableNames.length, 'written successfully')));
            }
        } catch (err) {
            console.log('sfFile check error:', err.message);
        }

        //check if TD file-cache exists locally
        let tdFileName = './td_data.csv';
        try {
            if (fs2.existsSync(tdFileName)) {
                //use the file
                if (!teradataColumnsFromTableNames) {
                    teradataColumnsFromTableNames = [];
                }
                fs2.createReadStream(tdFileName)
                    .pipe(csv())
                    .on('data', (row) => {
                        teradataColumnsFromTableNames.push(row);
                    }).on('end', () => {
                        console.log(tdFileName, 'successfully read-in:', teradataColumnsFromTableNames.length);
                        console.log('td record[0]:', teradataColumnsFromTableNames[0]);
                    })
            } else {
                //get data
                teradataColumnsFromTableNames = await loadTeradataData(table_names.slice(0, 4));
                console.log('TD column results:', teradataColumnsFromTableNames.length);
                console.log('td record[0]:', teradataColumnsFromTableNames[0]);
                //write the file
                const writer = require('csv-writer').createObjectCsvWriter({
                        path: tdFileName,
                        header: csvSchema
                    }).writeRecords(teradataColumnsFromTableNames)
                    .then(() => console.log(tdFileName, console.log('TD column results:', teradataColumnsFromTableNames.length, 'written successfully')));
            }
        } catch (err) {
            console.log('tdFile check error:', err.message);
        }


        //load Teradata Data for the table-names
        // let teradataColumnsFromTableNames1of4 = await loadTeradataData(table_names.slice(0, 4));
        // console.log('TD column results:', teradataColumnsFromTableNames1of4.length);

        // let dataTypesToSample = [{
        //     dataType: 'char',
        //     teradata: [''],
        //     snowflake: ['']
        // }, {
        //     dataType: 'number',
        //     teradata: [''],
        //     snowflake: ['']
        // }, {
        //     dataType: 'date',
        //     teradata: [''],
        //     snowflake: ['']
        // }, {
        //     dataType: 'timestamp_ntz',
        //     teradata: [''],
        //     snowflake: ['']
        // }];

        // -- UPDATE for Snowflake SQL generation --  
        // wholesale (Garpac) USA tables - Siva said TD-SF comparison requires
        // additional filtering in Snowflake where company_code = 1 
        // effectively limiting testing/validation scope 
        // for Wholesale to Company Code 1 data only
        // acceptable :)

        // MariaDB 
        /*
        // const pool = workerFunctions.connectionSetup(options.mariaDbConnectionConfigurations, "mariaDb");
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


    } catch (err) {
        console.log("err:", err);
        throw err;
    } finally {
        console.log('done, cleanup initiating...');

    }
}

asyncMain();
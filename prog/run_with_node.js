const { getSystemErrorMap } = require('util');

const mariadb = require('mariadb');
const fs = require("fs").promises;
const fs2 = require('fs');
const csv = require('csv-parser');
const csvToJson = require('csvtojson');
const _ = require('underscore');

const loadSnowflakeData = require('./snowflake/');
const loadTeradataData = require('./teradata/');
const { Console, table } = require('console');

async function findMatchByColumnName(columnName, matchCandidates) {
    let matches = [];

    for (var i = 0; i < matchCandidates.length; i++) {
        if (matchCandidates[i].ColumnName == columnName && matches.indexOf(columnName) < 0) {
            matches.push(matchCandidates[i]);
            // console.log('match:', matchCandidates[i].TableName, '.', columnName);
        } else {
            // console.log('non-match:', columnName, ' != ', matchCandidates[i].ColumnName);
        }
    }

    if (matches.length > 0) console.log('matches:', matches.length);

    return matches;
}

async function getDistinctTableNames(columnsRecords) {
    let result = [];

    //iterate tables in tdColumns
    for (var i = 0; i < columnsRecords.length; i++) {
        // console.log(tdColumns[i]);
        if (result.indexOf(columnsRecords[i].TableName) < 0) {
            result.push(columnsRecords[i].TableName);
            // console.log('pushed:', tdColumns[i]);
        }
    }

    return result;
}

async function getAggregatesSQL(columnDefinition) {

}

async function getFullCountSQL(columnDefinition) {
    let result = `insert into
    skx_audit_reference (
        group_id,
        rec_description,
        source1_table_name,
        source2_table_name,
        source3_table_name,
        source1_property,
        source2_property,
        source3_property,
        sql_return_type,
        source1_sql,
        source2_sql,
        source3_sql,
        processfrequency,
        isrowenabled,
        create_datetime,
        create_user
    )
    values
    (` + `
        14,
    'Full table count ` + columnDefinition.ContainerName;

    return result;
}

async function generateSQLForAuditReferenceTable(sfColumns, tdColumns) {
    console.log('td:', tdColumns[0]);
    console.log('sf:', sfColumns[0]);
    console.log('tdColumns.length:', tdColumns.length);
    console.log('sfColumns.length:', sfColumns.length);

    let distinctTableNames = await getDistinctTableNames(tdColumns);

    console.log('distinctTableNames (td):', distinctTableNames);

    let result = [];
    //iterate sfColumns
    for (var i = 0; i < sfColumns.length; i++) {
        sf = sfColumns[i];
        // console.log('sf:', sf.TableName);
        if (distinctTableNames.indexOf(sf.TableName >= 0)) {
            let tdMatchByColumnName = await findMatchByColumnName(sf.ColumnName, tdColumns);

            if (tdMatchByColumnName && tdMatchByColumnName.length > 0) {
                console.log(tdMatchByColumnName);
                //for each match on the column name, generate SQL

                // N.B. hard-code array of Garpac instances, to utilize in rec_description

                let sqlOutputForCount = getFullCountSQL(tdMatchByColumnName);
                let sqlOutputForAggregate = getAggregatesSQL(tdMatchByColumnName);
            } else {
                // console.log('no match in tdColumns for: ', sf.ColumnName);
            }
        }
    }
}

async function getTableNamesNotInFirstList(list_1, list_2) {
    return _.difference(list_2, list_1);
}

async function mergeUniqueTableNamesFromEachList(list1, list2) {
    return _.union(list1, list2);
}

async function asyncMain() {

    const loadData = true;

    try {
        //read the files for list of tables and other pertinent info
        let table_names = (await fs.readFile("./resources/table_list_DE877.csv", "utf-8")).split(/\r?\n/).slice(1);
        console.log('table_names (from DE-877 comment):', table_names.length);

        let wholesale_table_list_2_objects = await csvToJson()
            .fromFile("./resources/wholesale_tables_src_tgt.csv");
        console.log('wholesale_table_list_2_objects:', wholesale_table_list_2_objects.length);

        //compare the two lists by the pre-identified field [target_class_name] in the second csv->json
        let tableNames_list_2 = _.map(wholesale_table_list_2_objects, function(item) {
            return item.target_class_name.substring(item.target_class_name.indexOf('DW_RIPSAW.') + 10);
        });

        let tableNamesNotInFirstList = await getTableNamesNotInFirstList(table_names, tableNames_list_2);
        console.log('tableNamesNotInFirstList:', tableNamesNotInFirstList.length);

        let tableNamesNotInSecondList = await getTableNamesNotInFirstList(tableNames_list_2, table_names);
        console.log('tableNamesNotInSecondList:', tableNamesNotInSecondList.length);

        let uniqueTableNamesAcrossBothList = await mergeUniqueTableNamesFromEachList(table_names, tableNames_list_2);

        console.log('unique table-name count:', uniqueTableNamesAcrossBothList.length);
        console.log('list 2 table-name count:', tableNames_list_2.length);

        //prep the work.
        let snowflakeColumnsFromTableNames, teradataColumnsFromTableNames;
        let csvSchema = [
            { id: 'TABLE_SCHEMA', title: 'ContainerName' },
            { id: 'TABLE_NAME', title: 'TableName' },
            { id: 'COLUMN_NAME', title: 'ColumnName' },
            { id: 'DATA_TYPE', title: 'ColumnType' }
        ];

        if (loadData) {
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

                            generateSQLForAuditReferenceTable(snowflakeColumnsFromTableNames, teradataColumnsFromTableNames);
                        })
                } else {
                    //get data
                    snowflakeColumnsFromTableNames = await loadSnowflakeData(uniqueTableNamesAcrossBothList);
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

                            // if (moreTDColumInfoToGet(teradataColumnsFromTableNames)) {
                            //     //get more tables' column metadata
                            // }
                        })
                } else {
                    //get data for the first time, from TD - for 5 tables
                    teradataColumnsFromTableNames = await loadTeradataData(uniqueTableNamesAcrossBothList.slice(0, 4));
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
        }


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
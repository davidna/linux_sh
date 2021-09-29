const options = {
    host: 'ripsaw.skechers.com',
    log: '0',
    user: 'expadmin',
    password: 'scbltE!'
}

const TeradataConnection = require("teradata-nodejs-driver/teradata-connection");

async function loadData(tableNames) {
    let teradataDbSchemaQueryTables = `SELECT DatabaseName as SCHEMA_NAME, TableName as TABLE_NAME, ColumnName as COLUMN_NAME, ColumnFormat as DATA_TYPE FROM DBC.ColumnsV WHERE TableName IN (${tableNames.map(item => `'${item}'`).join(',')})
    order by DatabaseName, TableName, ColumnFormat, ColumnName;`;

    let connTeradataDb = new TeradataConnection.TeradataConnection();
    let cursor = connTeradataDb.cursor();
    connTeradataDb.connect(options);
    cursor.execute(teradataDbSchemaQueryTables);
    console.log('execute sent to TD cursor');

    let displayedIt = false;
    const results = Array.from({
        length: cursor.rowCount
    }, (_, i) => {
        const item = cursor.fetchone();
        if (!displayedIt) {
            console.log(i, item);
            console.log('TD table-schema:', item[0]);
            displayedIt = !displayedIt;
        }
        return {
            TABLE_SCHEMA: item[0].toString(),
            TABLE_NAME: item[1],
            COLUMN_NAME: item[2],
            DATA_TYPE: item[3] 
        }
    });

    // console.log('results.length:', results.length);

    cursor.close();
    connTeradataDb.close();

    return results;
}

module.exports = loadData;
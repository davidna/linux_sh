const options = {
    host: 'ripsaw.skechers.com',
    log: '0',
    user: 'expadmin',
    password: 'scbltE!'
}

const TeradataConnection = require("teradata-nodejs-driver/teradata-connection");

async function loadData(tableNames) {
    let teradataDbSchemaQueryTables = `SELECT DatabaseName, TableName, ColumnName, ColumnFormat FROM DBC.ColumnsV WHERE TableName IN (${tableNames.map(item => `'${item}'`).join(',')})
    order by DatabaseName, TableName, ColumnFormat, ColumnName;`;

    let connTeradataDb = new TeradataConnection.TeradataConnection();
    let cursor = connTeradataDb.cursor();
    connTeradataDb.connect(options);
    cursor.execute(teradataDbSchemaQueryTables);
    console.log('execute sent to TD cursor');

    const results = Array.from({
        length: cursor.rowCount
    }, (_, i) => {
        const item = cursor.fetchone();
        console.log(i, item);

        return item;
    });

    console.log('results.length:', results.length);

    cursor.close();
    connTeradataDb.close();

    return results;
}

module.exports = loadData;
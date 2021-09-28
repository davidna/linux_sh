var config = {
    account: "SKECHERS",
    username: "svc_erwin",
    password: "0^ag5L6C$0W2V1x28tOm9",
    database: "DB_STAGING",
    warehouse: "WH_BAT_ETL_XSMALL",
    role: "ROLE_DA_OPERATIONS"
};

// const SnowflakeSDK = require("snowflake-sdk");
const Snowflake = require('snowflake-promise').Snowflake;


async function loadData(tableNames) {

    const snowflake = new Snowflake(config);

    await snowflake.connect();

    let snowflakeDbSchemaQueryString = `
    SELECT table_schema, table_name, column_name, data_type
    FROM DB_STAGING.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME IN (${tableNames.map(item => `'${item}'`).join(',')})
    order by table_schema, table_name, data_type, column_name;
    `.replace(/\r?\n/g, " ").trim();

    // console.log('snowflakeDbSchemaQueryString:', snowflakeDbSchemaQueryString);

    let rows = await snowflake.execute(snowflakeDbSchemaQueryString);

    return rows;
}

module.exports = loadData;
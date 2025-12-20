package com.zhugeio.meta;


/**
 * @desc MySQL数据库 meta信息查询
 * @Author Liujh
 * @Date 2022/7/18 14:47
 */
public class MySqlDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

    private volatile static MySqlDatabaseMeta single;

    public static MySqlDatabaseMeta getInstance() {
        if (single == null) {
            synchronized (MySqlDatabaseMeta.class) {
                if (single == null) {
                    single = new MySqlDatabaseMeta();
                }
            }
        }
        return single;
    }

    @Override
    public String getSQLQueryComment(String schemaName, String tableName, String columnName) {
        return String.format("SELECT COLUMN_COMMENT FROM information_schema.COLUMNS where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' and COLUMN_NAME = '%s'", schemaName, tableName, columnName);
    }

    @Override
    public String getSQLQueryTables() {
        return "show tables;";
    }


    @Override
    public String showFullColumns(String tableName) {
        return "show full columns from " + tableName;
    }



}

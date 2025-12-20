package com.zhugeio.meta;


/**
 * Oracle数据库 meta信息查询
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName MySQLDatabaseMeta
 * @Version 1.0
 * @since 2019/7/17 15:48
 */
public class OracleDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

    private volatile static OracleDatabaseMeta single;

    public static OracleDatabaseMeta getInstance() {
        if (single == null) {
            synchronized (OracleDatabaseMeta.class) {
                if (single == null) {
                    single = new OracleDatabaseMeta();
                }
            }
        }
        return single;
    }


    @Override
    public String getSQLQueryComment(String schemaName, String tableName, String columnName) {
        return String.format("select B.comments \n" +
                "  from sys.dba_tab_columns A, sys.dba_col_comments B\n" +
                " where a.COLUMN_NAME = b.column_name\n" +
                "   and A.Table_Name = B.Table_Name\n" +
                "   and A.Table_Name = upper('%s')\n" +
                "   AND A.column_name  = '%s'", tableName, columnName);
    }



    @Override
    public String getSQLQueryTables() {
        return "select table_name from user_tables";
    }


    @Override
    public String showFullColumns(String tableName) {
        return "SELECT tab.COLUMN_NAME,col.COMMENTS,tab.DATA_TYPE,tab.DATA_LENGTH,tab.DATA_PRECISION,tab.NULLABLE FROM all_col_comments col, all_tab_columns tab\n" +
                "WHERE col.Table_Name = '" + tableName + "'  AND tab.Table_Name = '" + tableName + "' AND tab.COLUMN_NAME = col.COLUMN_NAME";
    }


}

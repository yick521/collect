package com.zhugeio.meta;

/**
 * Postgresql数据库 meta信息查询
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName PostgresqlDatabaseMeta
 * @Version 1.0
 * @since 2019/8/2 11:02
 */
public class PostgresqlDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

    private volatile static PostgresqlDatabaseMeta single;

    public static PostgresqlDatabaseMeta getInstance() {
        if (single == null) {
            synchronized (PostgresqlDatabaseMeta.class) {
                if (single == null) {
                    single = new PostgresqlDatabaseMeta();
                }
            }
        }
        return single;
    }



    @Override
    public String getSQLQueryTables() {
        return "select relname as tabname from pg_class c \n" +
                "where  relkind = 'r' and relname not like 'pg_%' and relname not like 'sql_%' group by relname order by relname limit 500";
    }



    @Override
    public String getSQLQueryComment(String schemaName, String tableName, String columnName) {
        return null;
    }
}

package com.zhugeio.meta;

import com.zhugeio.model.Constant;

/**
 * @desc meta信息工厂
 * @Author Liujh
 * @Date 2022/7/18 14:47
 */
public class DatabaseMetaFactory {

    //根据数据库类型返回对应的接口
    public static DatabaseInterface getByDbType(String dbType) {
        if (Constant.MYSQL_TYPE.equalsIgnoreCase(dbType) || Constant.ORACLE_TYPE.equalsIgnoreCase(dbType)
                || Constant.SQL_SERVER_TYPE.equalsIgnoreCase(dbType) || Constant.HBASE_DB_TYPE.equalsIgnoreCase(dbType)
                || Constant.OCEAN_BASE_TYPE.equalsIgnoreCase(dbType)) {
            return MySqlDatabaseMeta.getInstance();
        } else if (Constant.IMPALA_TYPE.equalsIgnoreCase(dbType) || Constant.IMPALA_ANALYTICS.equalsIgnoreCase(dbType)) {
            return ImpalaDatabaseMeta.getInstance();
        } else if (Constant.DORIS_TYPE.equalsIgnoreCase(dbType)) {
            return DorisDatabaseMeta.getInstance();
        }
        // else if (Contains.ORACLE_TYPE.equalsIgnoreCase(dbType)) {
        //     return OracleDatabaseMeta.getInstance();
        // }

        else if (Constant.POSTGRE_SQL_TYPE.equalsIgnoreCase(dbType)) {
            return PostgresqlDatabaseMeta.getInstance();
        }
        // else if (Contains.SQL_SERVER_TYPE.equalsIgnoreCase(dbType)) {
        //     return SqlServerDatabaseMeta.getInstance();
        // }
        else if (Constant.HIVE_TYPE.equalsIgnoreCase(dbType)) {
            return HiveDatabaseMeta.getInstance();
        } else {
            throw new RuntimeException("找不到该类型: ".concat(dbType));
        }
    }
}

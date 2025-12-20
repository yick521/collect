package com.zhugeio.utils;


import com.zhugeio.model.Constant;
import com.zhugeio.model.JobDatasource;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

/**
 * 查询工具的工厂
 * 工具类，获取单例实体
 *
 * @author yangyhx
 * @Version 1.0
 * @date 2021/03/17
 */
@Slf4j
public class QueryToolFactory {

    public static BaseQueryTool getByDbType(JobDatasource jobDatasource) {
        //获取dbType
        String datasource = jobDatasource.getDatasourceType();

        if (datasource.equalsIgnoreCase(Constant.MYSQL_TYPE) || datasource.equalsIgnoreCase(Constant.ORACLE_TYPE)
                || datasource.equalsIgnoreCase(Constant.SQL_SERVER_TYPE) || datasource.equalsIgnoreCase(Constant.STARROCKS_TYPE)
                || datasource.equalsIgnoreCase(Constant.HBASE_DB_TYPE) || datasource.equalsIgnoreCase(Constant.OCEAN_BASE_TYPE)) {
            return getMySQLQueryToolInstance(jobDatasource);
        }
        // else if(datasource.equalsIgnoreCase(Contains.ORACLE_TYPE)){
        //     return getOracleQueryToolInstance(jobDatasource);
        // }
        else if (datasource.equalsIgnoreCase(Constant.POSTGRE_SQL_TYPE)) {
            return getPostgresqlQueryToolInstance(jobDatasource);
        } else if (datasource.equalsIgnoreCase(Constant.IMPALA_TYPE) || datasource.equalsIgnoreCase(Constant.IMPALA_ANALYTICS)) {
            return getImpalaJdbcToolInstance(jobDatasource);
        } else if (datasource.equalsIgnoreCase(Constant.DORIS_TYPE)) {
            return getDorisueryToolInstance(jobDatasource);
        } else if (datasource.equalsIgnoreCase(Constant.HIVE_TYPE)) {
            return getHiveQueryToolInstance(jobDatasource);
        }

        // else if(datasource.equalsIgnoreCase(Contains.SQL_SERVER_TYPE)){
        //     return getSqlserverQueryToolInstance(jobDatasource);
        // }


        throw new RuntimeException("找不到该类型: ".concat(datasource));
    }

    private static BaseQueryTool getImpalaJdbcToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new ImpalaJdbcTool(jdbcDatasource);
        } catch (Exception e) {
            throw new RuntimeException("您所选的数据库连接失败：" + e.getLocalizedMessage());
        }
    }

    private static BaseQueryTool getMySQLQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new MySQLQueryTool(jdbcDatasource);
        } catch (Exception e) {
            throw new RuntimeException("您所选的数据库连接失败：" + e.getLocalizedMessage());
        }
    }


    private static BaseQueryTool getDorisueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new DorisQueryTool(jdbcDatasource);
        } catch (Exception e) {
            throw new RuntimeException("您所选的数据库连接失败：" + e.getLocalizedMessage());
        }
    }

    private static BaseQueryTool getPostgresqlQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new PostgresqlQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw new RuntimeException("您所选的数据库连接失败：" + e.getLocalizedMessage());
        }
    }

    private static BaseQueryTool getHiveQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new HiveQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw new RuntimeException("您所选的数据库连接失败：" + e.getLocalizedMessage());
        }
    }

    public static BaseQueryTool getImpala(String impalaType) {
        log.info("初始化 【{}】 impala连接", impalaType);
        JobDatasource jobDatasource = new JobDatasource();
        jobDatasource.setJdbcDriverClass(Constant.IMPALA_DRIVE_CLASS);
        jobDatasource.setDatasourceType(impalaType);
        return QueryToolFactory.getByDbType(jobDatasource);
    }


    public static BaseQueryTool getDoris(String impalaType) {
        log.info("初始化 【{}】 impala连接", impalaType);
        JobDatasource jobDatasource = new JobDatasource();
        jobDatasource.setJdbcDriverClass(Constant.IMPALA_DRIVE_CLASS);
        jobDatasource.setDatasourceType(impalaType);
        return QueryToolFactory.getByDbType(jobDatasource);
    }

}

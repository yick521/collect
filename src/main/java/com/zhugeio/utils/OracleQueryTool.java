package com.zhugeio.utils;


import com.zhugeio.model.ColumnInfo;
import com.zhugeio.model.JobDatasource;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Oracle数据库使用的查询工具
 *
 * @author yangyhx
 * @date 2021/03/25
 */
@Slf4j
public class OracleQueryTool {

    public OracleQueryTool(JobDatasource jobDatasource) {
        try {
            getDataSource(jobDatasource);
        } catch (Exception e) {
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }

    private void getDataSource(JobDatasource jobDatasource)  {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(jobDatasource.getJdbcDriverClass());
            String username = jobDatasource.getJdbcUsername();
            if (username.equals("sys")||username.equals("system")){     //
                username = username + " as SYSDBA";
            }
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl(),username,jobDatasource.getJdbcPassword());
            stmt = conn.createStatement();
            String testSQL = "select 1 from dual";
            ResultSet resultSet = stmt.executeQuery(testSQL);
            while (resultSet.next()){
                log.info("connection init success");
                break;
            }
            resultSet.close();
            stmt.close();
            conn.close();
        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e);
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }

    public List<String> getTables(JobDatasource jobDatasource) {
        Connection conn = null;
        Statement stmt = null;
        List<String> tables = new ArrayList<>();
        try {
            Class.forName(jobDatasource.getJdbcDriverClass());
            String username = jobDatasource.getJdbcUsername();
            if (username.equals("sys")||username.equals("system")){     //
                username = username + " as SYSDBA";
            }
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl(),username,jobDatasource.getJdbcPassword());
            stmt = conn.createStatement();
            String tableSql = "select table_name from user_tables WHERE TABLESPACE_NAME NOT IN ('SYS','SYSTEM','SYSAUX')";
            log.info(tableSql);
            ResultSet resultSet = stmt.executeQuery(tableSql);
            while (resultSet.next()){
                tables.add(resultSet.getString("table_name"));
            }
            resultSet.close();
            stmt.close();
            conn.close();
        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e);
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
        return tables;
    }

    public List<ColumnInfo> getColumns(JobDatasource jobDatasource, String tableName) {
        Connection conn = null;
        Statement stmt = null;
        List<ColumnInfo> columns = new ArrayList<>();
        try {
            Class.forName(jobDatasource.getJdbcDriverClass());
            String username = jobDatasource.getJdbcUsername();
            if (username.equals("sys")||username.equals("system")){     //
                username = username + " as SYSDBA";
            }
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl(),username,jobDatasource.getJdbcPassword());
            stmt = conn.createStatement();
            String tableSql = "select COLUMN_NAME,DATA_TYPE from all_tab_columns  WHERE TABLE_NAME = '"+tableName+"'";
            log.info(tableSql);
            ResultSet resultSet = stmt.executeQuery(tableSql);
            while (resultSet.next()){
                ColumnInfo columnInfo = new ColumnInfo();
                columnInfo.setName(resultSet.getString("COLUMN_NAME"));
                columnInfo.setType(resultSet.getString("DATA_TYPE"));
                columns.add(columnInfo);
            }
            resultSet.close();
            stmt.close();
            conn.close();
        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e);
            throw new RuntimeException("您所选的数据库连接失败：" + e.getMessage());
        }
        return columns;
    }
}
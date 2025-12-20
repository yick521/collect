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
 * @ClassName DrdsQueryTool
 * @Desc TODO
 * @Author daijinlin
 * @Date 2023/4/11 16:05
 */
@Slf4j
public class DrdsQueryTool {

    public DrdsQueryTool(JobDatasource jobDatasource)  {
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
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl(),jobDatasource.getJdbcUsername(),jobDatasource.getJdbcPassword());
            stmt = conn.createStatement();
            String testSQL = "SELECT tablename FROM pg_tables";
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

    public List<String> getTables(JobDatasource jobDatasource)  {
        Connection conn = null;
        Statement stmt = null;
        List<String> tables = new ArrayList<>();
        try {
            Class.forName(jobDatasource.getJdbcDriverClass());
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl(),jobDatasource.getJdbcUsername(),jobDatasource.getJdbcPassword());
            stmt = conn.createStatement();
            String testSQL = "SELECT tablename FROM pg_tables where schemaname='public'";
            ResultSet resultSet = stmt.executeQuery(testSQL);
            while (resultSet.next()){
                tables.add(resultSet.getString("tablename"));
            }
            resultSet.close();
            stmt.close();
            conn.close();
        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e.getMessage());
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
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl(),jobDatasource.getJdbcUsername(),jobDatasource.getJdbcPassword());
            stmt = conn.createStatement();
            String testSQL = "select col.column_name, col.data_type from information_schema.columns col where table_schema = 'public' and table_name = '"+tableName+"';";
            ResultSet resultSet = stmt.executeQuery(testSQL);
            while (resultSet.next()){
                ColumnInfo columnInfo = new ColumnInfo();
                columnInfo.setName(resultSet.getString("column_name"));
                columnInfo.setType(resultSet.getString("data_type"));
                columns.add(columnInfo);
            }
            resultSet.close();
            stmt.close();
            conn.close();
        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e.getMessage());
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
        return columns;
    }
}

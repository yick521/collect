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
 * @ClassName ClickhouseQueryTool
 * @Desc TODO
 * @Author daijinlin
 * @Date 2023/4/10 14:41
 */
@Slf4j
public class ClickhouseQueryTool {
    public ClickhouseQueryTool(JobDatasource jobDatasource)  {
        try {
            getDataSource(jobDatasource);
        } catch (Exception e) {
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }

    private void getDataSource(JobDatasource jobDatasource)  {
        Connection conn = null;
        try {
            Class.forName(jobDatasource.getJdbcDriverClass());
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl());
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT name FROM system.tables Where database = 'default'");
            resultSet.close();
            statement.close();
            conn.close();

        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e.getMessage());
                throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }

    public List<String> getTables(JobDatasource jobDatasource) {
        Connection conn = null;
        Statement stmt = null;
        List<String> tables = new ArrayList<>();
        try {
            Class.forName(jobDatasource.getJdbcDriverClass());
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl(),jobDatasource.getJdbcUsername(),jobDatasource.getJdbcPassword());
            stmt = conn.createStatement();
            String tableSQL = "SELECT name FROM system.tables Where database = 'default'";
            ResultSet resultSet = stmt.executeQuery(tableSQL);
            while (resultSet.next()){
                String name = resultSet.getString("name");
                tables.add(name);
//                log.info("connection init success");
//                log.info(resultSet.getString("name"));
//                break;
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
            String testSQL = "select name,type from system.columns where table = '"+tableName+"' and database = 'default';";
            ResultSet resultSet = stmt.executeQuery(testSQL);
            while (resultSet.next()){
                String name = resultSet.getString("name");
                String type = resultSet.getString("type");
                ColumnInfo columnInfo = new ColumnInfo();
                columnInfo.setName(name);
                columnInfo.setType(type);
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

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
 * @ClassName KingbaseQueryTool
 * @Desc TODO
 * @Author daijinlin
 * @Date 2023/4/10 15:06
 */
@Slf4j
public class KingbaseQueryTool{

    /**
     * 用于获取查询语句
     */
    public KingbaseQueryTool(JobDatasource jobDatasource) {
        try {
            getDataSource(jobDatasource);
        } catch (Exception e) {
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }

    private void getDataSource(JobDatasource jobDatasource) {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName(jobDatasource.getJdbcDriverClass());
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl(), jobDatasource.getJdbcUsername(), jobDatasource.getJdbcPassword());
            stmt = conn.createStatement();
            String testSQL = "select 1 from dual";
            ResultSet resultSet = stmt.executeQuery(testSQL);
            while (resultSet.next()) {
                log.info("connection init success");
                break;
            }
            resultSet.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            log.error("您所选的数据库连接失败",e.getMessage());
            throw new RuntimeException("您所选的数据库连接失败：" + e.getMessage());
        }
    }

    public List<String> getTables(JobDatasource jobDatasource) {
        Connection conn = null;
        Statement stmt = null;
        List<String> tables = new ArrayList<>();
        try {
            Class.forName(jobDatasource.getJdbcDriverClass());
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl(), jobDatasource.getJdbcUsername(), jobDatasource.getJdbcPassword());
            stmt = conn.createStatement();
            String testSQL = "SELECT table_name FROM information_schema.TABLES WHERE table_schema ='public'";
            ResultSet resultSet = stmt.executeQuery(testSQL);
            while (resultSet.next()) {
//                log.info("connection init success");
                String name = resultSet.getString("table_name");
                tables.add(name);
//                break;
            }
            resultSet.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            log.error("您所选的数据库连接失败",e.getMessage());
            throw new RuntimeException("您所选的数据库连接失败：" + e.getMessage());
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
            String testSQL = "SELECT col.COLUMN_NAME, col.UDT_NAME FROM information_schema.COLUMNS col " +
                    " WHERE col.table_schema = 'public' " +
                    " AND col.TABLE_NAME = '"+tableName+"'; ";
            ResultSet resultSet = stmt.executeQuery(testSQL);
            while (resultSet.next()){
                String name = resultSet.getString("COLUMN_NAME");
                String type = resultSet.getString("UDT_NAME");
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

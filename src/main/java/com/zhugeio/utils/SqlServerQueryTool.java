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
 * sql server
 *
 * @author zhouhongfa@gz-yibo.com
 * @version 1.0
 * @since 2019/8/2
 */
@Slf4j
public class SqlServerQueryTool {

    public SqlServerQueryTool(JobDatasource jobDatasource) {
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
            String testSQL = "SELECT name FROM sys.sysdatabases";
            ResultSet resultSet = stmt.executeQuery(testSQL);
            while (resultSet.next()){
                log.info("connection init success");
//                log.info(resultSet.getString("name"));
                break;
            }
            resultSet.close();
            stmt.close();
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
            String testSQL = "SELECT Name FROM "+jobDatasource.getDatabaseName()+"..SysObjects Where XType='U' ORDER BY Name";
            ResultSet resultSet = stmt.executeQuery(testSQL);
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

    public List<ColumnInfo> getColumns(JobDatasource jobDatasource,String tableName) {
        Connection conn = null;
        Statement stmt = null;
        List<ColumnInfo> columns = new ArrayList<>();
        try {
            Class.forName(jobDatasource.getJdbcDriverClass());
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl(),jobDatasource.getJdbcUsername(),jobDatasource.getJdbcPassword());
            stmt = conn.createStatement();
            String testSQL = "Select  " +
                    "  name=rtrim(b.name),  " +
                    "  type=type_name(b.xusertype)+CASE WHEN b.colstat&1=1 " +
                    "  THEN '[ID(' + CONVERT(varchar, ident_seed(a.name))+','+CONVERT(varchar,ident_incr(a.name))+')]' " +
                    "  ELSE '' END  " +
                    "FROM sysobjects a, syscolumns b  " +
                    "Where (a.id=b.id)AND(a.id=object_id('"+tableName+"')) " +
                    "order BY b.colid ";
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
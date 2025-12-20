package com.zhugeio.utils;

import com.zhugeio.model.JobDatasource;
import lombok.extern.slf4j.Slf4j;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName ImpalaQueryTool
 * @Desc TODO
 * @Author daijinlin
 * @Date 2023/5/18 16:29
 */
@Slf4j
public class ImpalaQueryTool {

    public ImpalaQueryTool(JobDatasource jobDatasource) {
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
            String testSQL = "show tables";
            ResultSet resultSet = stmt.executeQuery(testSQL);
            while (resultSet.next()){
                String name = resultSet.getString("name");
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

    public List<String> getCollectionNames(JobDatasource jobDatasource) {
        Connection conn = null;
        Statement stmt = null;
        List<String> result = new ArrayList<>();
        try {
            Class.forName(jobDatasource.getJdbcDriverClass());
            conn = DriverManager.getConnection(jobDatasource.getJdbcUrl());
            stmt = conn.createStatement();
            String tablesSql = "show tables";
            ResultSet resultSet = stmt.executeQuery(tablesSql);
            while (resultSet.next()){
                result.add(resultSet.getString("name"));
            }
            resultSet.close();
            stmt.close();
            conn.close();
        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e);
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
        return result;
    }
}

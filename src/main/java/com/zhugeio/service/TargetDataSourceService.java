package com.zhugeio.service;

import com.zhugeio.config.SyncTaskConfig;
import com.zhugeio.model.IncrementInfo;
import com.zhugeio.model.TaskConfig;
import com.zhugeio.utils.AESUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.sql.*;

/**
 * 目标数据源查询服务
 * 用于查询Impala/Doris中的表结构和增量数据
 */
@Slf4j
@Service
public class TargetDataSourceService {

    @Autowired
    private SyncTaskConfig syncTaskConfig;

    @Autowired
    private AESUtils aesUtils;



    /**
     * 查询增量数据的最大值
     */
    public IncrementInfo queryIncrementMaxValue(String tableName, String increColumn, boolean isTimeType) {
        String sql;
        if (isTimeType) {
            sql = "select concat('''', nvl(max(" + increColumn + "),'1970-01-01 00:00:00'), '''') as maxTime from " + tableName;
        } else {
            sql = "select nvl(max(" + increColumn + "),0) as maxid from " + tableName;
        }

        log.info("执行增量查询SQL: {}", sql);

        try (Connection conn = getTargetConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            if (rs.next()) {
                IncrementInfo incrementInfo = new IncrementInfo();
                if (isTimeType) {
                    String maxTime = rs.getString("maxTime");
                    incrementInfo.setMaxTime(maxTime);
                    incrementInfo.setPlaceholder("{maxtime}");
                    incrementInfo.setRealValue(maxTime);
                    incrementInfo.setTimeType(true);
                    log.info("查询到最大时间值: {}", maxTime);
                } else {
                    long maxId = rs.getLong("maxid");
                    incrementInfo.setMaxId(maxId);
                    incrementInfo.setPlaceholder("{maxid}");
                    incrementInfo.setRealValue(String.valueOf(maxId));
                    incrementInfo.setTimeType(false);
                    log.info("查询到最大ID值: {}", maxId);
                }
                return incrementInfo;
            }

        } catch (Exception e) {
            log.error("查询增量最大值失败 - 表: {}, 字段: {}, 错误: {}", tableName, increColumn, e.getMessage(), e);
        }

        // 返回默认值
        IncrementInfo defaultInfo = new IncrementInfo();
        if (isTimeType) {
            defaultInfo.setMaxTime("'1970-01-01 00:00:00'");
            defaultInfo.setPlaceholder("{maxtime}");
            defaultInfo.setRealValue("'1970-01-01 00:00:00'");
            defaultInfo.setTimeType(true);
        } else {
            defaultInfo.setMaxId(0L);
            defaultInfo.setPlaceholder("{maxid}");
            defaultInfo.setRealValue("0");
            defaultInfo.setTimeType(false);
        }
        return defaultInfo;
    }


    // 添加清空目标表的方法
    public void clearTargetTable(TaskConfig taskConfig) throws Exception {
        String targetTable = taskConfig.getTargetTable();
        if (!targetTable.contains(".")) {
            targetTable = taskConfig.getTargetDb().getDbName() + "." + targetTable;
        }

        String clearSql = "TRUNCATE TABLE " + targetTable;
        log.info("执行清空表SQL: {}", clearSql);

        try (Connection conn = getTargetConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(clearSql);
            log.info("成功清空目标表: {}", targetTable);
        } catch (Exception e) {
            log.error("清空目标表失败 - 表: {}, 错误: {}", targetTable, e.getMessage(), e);
            throw e;
        }
    }

    // 刷新元数据
    public void refreshTargetTable(TaskConfig taskConfig) throws Exception {
        if(syncTaskConfig.getTargetDataSource().getDbtype() == 1) {
            String targetTable = taskConfig.getTargetTable();
            if (!targetTable.contains(".")) {
                targetTable = taskConfig.getTargetDb().getDbName() + "." + targetTable;
            }

            String clearSql = "Invalidate Metadata " + targetTable;
            log.info("执行刷新元数据SQL: {}", clearSql);

            try (Connection conn = getTargetConnection();
                 Statement stmt = conn.createStatement()) {

                stmt.execute(clearSql);
                log.info("成功刷新元数据: {}", targetTable);
            } catch (Exception e) {
                log.error("刷新元数据失败 - 表: {}, 错误: {}", targetTable, e.getMessage(), e);
                throw e;
            }
        }

    }

    /**
     * 获取目标数据源连接
     */
    private Connection getTargetConnection() throws SQLException, ClassNotFoundException {
        SyncTaskConfig.TargetDataSource target = syncTaskConfig.getTargetDataSource();

        try {
            // 加载驱动
            Class.forName(target.getDriverClassName());
            log.debug("成功加载驱动: {}", target.getDriverClassName());
        } catch (ClassNotFoundException e) {
            log.error("无法加载JDBC驱动: {}, 请检查驱动包是否在classpath中", target.getDriverClassName());
            throw e;
        }

        Connection conn = null;
        try {
            // 创建连接
            log.debug("尝试连接数据库: {}", target.getJdbcUrl());
            conn = DriverManager.getConnection(
                    target.getJdbcUrl(),
                    target.getUsername(),
                    target.getPassword()
            );


            log.debug("成功连接到目标数据源: {}", target.getJdbcUrl());
            return conn;

        } catch (SQLException e) {
            log.error("连接数据库失败 - URL: {}, 用户: {}, 错误: {}",
                    target.getJdbcUrl(), target.getUsername(), e.getMessage());
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException closeException) {
                    log.warn("关闭连接时发生异常: {}", closeException.getMessage());
                }
            }
            throw e;
        }
    }


}
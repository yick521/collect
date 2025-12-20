package com.zhugeio.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 同步任务配置
 */
@Data
@Component
@ConfigurationProperties(prefix = "datax.sync")
public class SyncTaskConfig {

    /**
     * DataX Python执行路径
     */
    private String dataxPythonPath = "python /opt/datax/bin/datax.py";

    /**
     * 临时文件存储路径
     */
    private String tempFilePath = "/tmp/datax";

    /**
     * 是否启用增量同步
     */
    private boolean enableIncrement = true;

    /**
     * 是否启用调试模式
     */
    private boolean debug = false;

    /**
     * 数据库连接超时时间（秒）
     */
    private int connectionTimeout = 30;

    /**
     * 查询超时时间（秒）
     */
    private int queryTimeout = 60;

    /**
     * HDFS默认文件系统地址
     */
    private String hdfsDefaultFS = "hdfs://localhost:9000";

    private String dorisLoadUrl = "";

    /**
     * 默认数据源配置
     */
    private TargetDataSource targetDataSource = new TargetDataSource();

    @Data
    public static class TargetDataSource {
        /**
         * 目标数据源类型: 1:IMPALA, 2:DORIS, 3:
         */
        private int dbtype = 2;

        /**
         * JDBC URL
         */
        private String jdbcUrl = "jdbc:impala://localhost:21050/default";

        /**
         * 用户名
         */
        private String username = "impala";

        /**
         * 密码
         */
        private String password = "";

        /**
         * 驱动类名
         */
        private String driverClassName = "";



        /**
         * 获取正确的驱动类名
         */
        public String getDriverClassName() {
            return this.driverClassName;
        }
    }
}
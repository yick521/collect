package com.zhugeio.model;
import lombok.Data;
import java.util.List;

/**
 * 任务配置模型
 * 对应task.json文件的数据结构
 */
@Data
public class TaskConfig {

    /**
     * 任务ID
     */
    private Long id;

    /**
     * 任务名称
     */
    private String name;

    /**
     * 任务类型
     */
    private String type;

    /**
     * 调度周期类型：DAY, HOUR, WEEK等
     */
    private String cycleType;

    /**
     * 优先级
     */
    private Integer priority;

    /**
     * 最大重试次数
     */
    private Integer retryMax;

    /**
     * 重试间隔（分钟）
     */
    private Integer retryDur;

    /**
     * 调度时间配置JSON字符串
     */
    private String schedulerTime;

    /**
     * 源表名
     */
    private String sourceTable;

    /**
     * 目标表名
     */
    private String targetTable;

    /**
     * 源数据库配置
     */
    private DataSourceInfo sourceDb;

    /**
     * 目标数据库配置
     */
    private DataSourceInfo targetDb;

    /**
     * 列配置信息
     */
    private ColumnDto columnDto;

    /**
     * 数据源信息
     */
    @Data
    public static class DataSourceInfo {
        private Long id;
        private String dsType;
        private String dsName;
        private String dsUrl;
        private String dsUser;
        private String encryptPwd;
        private String pwdKey;
        private String dbHost;
        private String dbPort;
        private String dbName;
        private String status;
    }

    /**
     * 列配置信息
     */
    @Data
    public static class ColumnDto {
        /**
         * 增量字段名
         */
        private String incrementColumn;

        /**
         * 增量类型：ALL(全量), INCREMENT(增量)
         */
        private String incrementType;

        /**
         * 是否选择所有列
         */
        private Boolean isAllColumn;

        /**
         * 分片键
         */
        private String splitPk;

        /**
         * WHERE条件
         */
        private String where;

        /**
         * 源表列名列表
         */
        private List<String> sourceColumns;

        /**
         * 目标表列名列表
         */
        private List<String> targetColumns;
    }
}
package com.zhugeio.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhugeio.model.ColumnInfo;
import com.zhugeio.model.JobDatasource;
import com.zhugeio.model.TaskConfig;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 简化版列构建器
 * 直接使用 TaskConfig.ColumnDto 中已解析的字段配置
 * 不再需要查询数据库或过滤字段
 */
@Slf4j
public class TaskColumnBuilder {

    /**
     * 构建列配置 - 核心方法
     * 直接使用 columnDto 中已解析好的字段配置
     */
    public ColumnResult buildColumnsForTask(TaskConfig taskConfig) {
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();

        if (columnDto == null) {
            log.warn("ColumnDto 为空，返回空结果");
            return new ColumnResult("", "");
        }

        List<String> sourceColumns = columnDto.getSourceColumns();
        List<String> targetColumns = columnDto.getTargetColumns();
        String sourceDbType = taskConfig.getSourceDb().getDsType().toLowerCase();

        // 构建 source columns（用于 reader）
        String dbColumn = buildDbColumn(sourceColumns, sourceDbType);

        // 构建 target columns（用于 writer）
        String hiveColumn = buildHiveColumn(targetColumns);

        log.info("列配置构建完成 - sourceType: {}, sourceColumns: {}, targetColumns: {}",
                sourceDbType,
                sourceColumns != null ? sourceColumns.size() : 0,
                targetColumns != null ? targetColumns.size() : 0);

        return new ColumnResult(hiveColumn, dbColumn);
    }

    /**
     * 构建 DB Column（用于 Reader）
     * 根据源数据库类型生成对应格式的列配置
     */
    private String buildDbColumn(List<String> sourceColumns, String sourceDbType) {
        if (sourceColumns == null || sourceColumns.isEmpty()) {
            return "";
        }

        // 根据数据源类型决定格式
        switch (sourceDbType) {
            case "mysql":
            case "oceanbase":
            case "starrocks":
            case "drds":
            case "sqlserver":
                // 关系型数据库："`column1`","`column2`"
                return buildRelationalDbColumn(sourceColumns);

            case "postgresql":
            case "kingbase":
            case "oracle":
                // PostgreSQL/Oracle："column1","column2"
                return buildPostgresColumn(sourceColumns);

            case "mongodb":
            case "hbase":
                // MongoDB/HBase：{"name":"xxx","type":"xxx"}
                return buildJsonColumn(sourceColumns);

            case "hive":
            case "impala":
            case "hdfs":
                // HDFS/Hive：{"index":0,"type":"String"}
                return buildIndexColumn(sourceColumns);

            default:
                log.warn("未知的数据源类型: {}, 使用默认处理", sourceDbType);
                return buildRelationalDbColumn(sourceColumns);
        }
    }

    /**
     * 构建 Hive Column（用于 Writer）
     * targetColumns 中可能包含完整的 JSON 配置或纯字段名
     */
    private String buildHiveColumn(List<String> targetColumns) {
        if (targetColumns == null || targetColumns.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < targetColumns.size(); i++) {
            String columnConfig = targetColumns.get(i);

            if (i > 0) {
                sb.append(",");
            }

            // 尝试解析为 JSON 对象（带类型信息）
            try {
                JSONObject columnObj = JSON.parseObject(columnConfig);
                String name = columnObj.getString("name");
                String type = columnObj.getString("type");

                // 清理字段名（去掉反引号）
                name = cleanColumnName(name);

                // 映射到 Hive 类型
                String hiveType = mapToHiveType(type);

                sb.append("{\"name\":\"`").append(name).append("`\",\"type\":\"")
                        .append(hiveType).append("\"}");

            } catch (Exception e) {
                // 解析失败，说明是纯字段名
                String columnName = cleanColumnName(columnConfig);
                sb.append("{\"name\":\"`").append(columnName).append("`\",\"type\":\"string\"}");
            }
        }

        return sb.toString();
    }

    /**
     * 构建关系型数据库列配置
     * 格式："`column1`","`column2`"
     */
    private String buildRelationalDbColumn(List<String> columns) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);

            if (i > 0) {
                sb.append(",");
            }

            // 如果已经有反引号，直接使用；否则添加
            if (column.startsWith("`")) {
                sb.append("\"").append(column).append("\"");
            } else {
                sb.append("\"`").append(column).append("`\"");
            }
        }

        return sb.toString();
    }

    /**
     * 构建 PostgreSQL/Oracle 列配置
     * 格式："column1","column2"
     */
    private String buildPostgresColumn(List<String> columns) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append("\"").append(cleanColumnName(columns.get(i))).append("\"");
        }

        return sb.toString();
    }

    /**
     * 构建 JSON 格式列配置（MongoDB/HBase）
     * 格式：{"name":"xxx","type":"xxx"}
     */
    private String buildJsonColumn(List<String> columns) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            String columnConfig = columns.get(i);

            if (i > 0) {
                sb.append(",");
            }

            // 检查是否已经是 JSON 格式
            if (columnConfig.trim().startsWith("{")) {
                // 已经是 JSON，直接使用
                sb.append(columnConfig);
            } else {
                // 纯字段名，构建 JSON
                String name = cleanColumnName(columnConfig);
                sb.append("{\"name\":\"").append(name).append("\",\"type\":\"string\"}");
            }
        }

        return sb.toString();
    }

    /**
     * 构建索引格式列配置（HDFS/Hive）
     * 格式：{"index":0,"type":"String"}
     */
    private String buildIndexColumn(List<String> columns) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            String columnConfig = columns.get(i);

            if (i > 0) {
                sb.append(",");
            }

            // 检查是否已经是 JSON 格式
            if (columnConfig.trim().startsWith("{")) {
                // 已经是 JSON 格式（{"index":0,"type":"String"}），直接使用
                sb.append(columnConfig);
            } else {
                // 纯字段名或索引，构建 JSON
                sb.append("{\"index\":").append(i).append(",\"type\":\"String\"}");
            }
        }

        return sb.toString();
    }

    /**
     * 映射 DataX 类型到 Hive 类型
     */
    private String mapToHiveType(String dataxType) {
        if (StringUtils.isBlank(dataxType)) {
            return "string";
        }

        switch (dataxType.toUpperCase()) {
            case "STRING":
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
                return "string";
            case "BIGINT":
            case "LONG":
                return "bigint";
            case "INT":
            case "INTEGER":
                return "int";
            case "DOUBLE":
                return "double";
            case "FLOAT":
                return "float";
            case "BOOLEAN":
                return "boolean";
            case "TIMESTAMP":
            case "DATETIME":
                return "timestamp";
            case "DATE":
                return "date";
            case "DECIMAL":
                return "decimal";
            default:
                return "string";
        }
    }

    /**
     * 清理字段名（去掉反引号、引号等）
     */
    private String cleanColumnName(String columnName) {
        if (StringUtils.isBlank(columnName)) {
            return columnName;
        }
        return columnName.replace("`", "").replace("\"", "").replace("'", "").trim();
    }

    // ====================================================================
    // 以下方法用于需要从数据库查询表结构的场景（如创建目标表）
    // ====================================================================

    /**
     * 获取表的字段信息
     * 使用项目现有的QueryTool体系从数据库查询表结构
     */
    public List<ColumnInfo> getTableColumns(TaskConfig taskConfig) {
        JobDatasource jobDatasource = buildJobDatasource(taskConfig.getSourceDb(), taskConfig.getSourceTable());
        log.info("getTableColumns jobDatasource: {}", jobDatasource);

        String dbType = taskConfig.getSourceDb().getDsType().toLowerCase();
        String tableName = taskConfig.getSourceTable();

        List<ColumnInfo> columns = new ArrayList<>();

        try {
            switch (dbType) {
                case "mysql":
                case "oceanbase":
                case "starrocks":
                case "postgresql":
                case "doris": {
                    BaseQueryTool queryTool = QueryToolFactory.getByDbType(jobDatasource);
                    columns = queryTool.getColumns(tableName);
                    queryTool.closeCon();
                    break;
                }
                case "impala":
                case "hive": {
                    BaseQueryTool queryTool = QueryToolFactory.getByDbType(jobDatasource);
                    // hive 这里要截取表名
                    String splitTableName = tableName;
                    String[] split = tableName.split("/");
                    if (split != null && split.length > 0)
                        splitTableName = split[split.length - 1];
                    columns = queryTool.getColumns(splitTableName);
                    queryTool.closeCon();
                    break;
                }
                case "sqlserver": {
                    SqlServerQueryTool sqlServerQueryTool = new SqlServerQueryTool(jobDatasource);
                    columns = sqlServerQueryTool.getColumns(jobDatasource, tableName);
                    break;
                }
                case "mongodb": {
                    MongoDBQueryTool mongoTool = new MongoDBQueryTool(jobDatasource);
                    columns = mongoTool.getColumns(tableName);
                    MongoDBQueryTool.sourceClose();
                    break;
                }
                case "oracle": {
                    OracleQueryTool oracleQueryTool = new OracleQueryTool(jobDatasource);
                    columns = oracleQueryTool.getColumns(jobDatasource, tableName);
                    break;
                }
                case "clickhouse": {
                    ClickhouseQueryTool clickhouseTool = new ClickhouseQueryTool(jobDatasource);
                    columns = clickhouseTool.getColumns(jobDatasource, tableName);
                    break;
                }
                case "drds": {
                    DrdsQueryTool drdsTool = new DrdsQueryTool(jobDatasource);
                    columns = drdsTool.getColumns(jobDatasource, tableName);
                    break;
                }
                case "hbase": {
                    HBaseQueryTool hbaseTool = new HBaseQueryTool(jobDatasource);
                    columns = hbaseTool.getColumns(jobDatasource, tableName);
                    break;
                }
                case "kingbase": {
                    KingbaseQueryTool kingbaseTool = new KingbaseQueryTool(jobDatasource);
                    columns = kingbaseTool.getColumns(jobDatasource, tableName);
                    break;
                }
                case "hdfs": {
                    // HDFS 类型：从 columnDto 构建字段信息
                    List<String> sourceColumns = taskConfig.getColumnDto().getSourceColumns();
                    for (String columnName : sourceColumns) {
                        ColumnInfo columnInfo = new ColumnInfo();
                        columnInfo.setName(columnName);
                        columnInfo.setType("string");
                        columnInfo.setComment(columnName);
                        columns.add(columnInfo);
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("不支持的数据库类型: " + dbType);
            }

        } catch (Exception e) {
            throw new RuntimeException("获取表结构失败: " + e.getMessage(), e);
        }

        return columns;
    }

    /**
     * 根据TaskConfig过滤需要的字段
     */
    public List<ColumnInfo> filterSelectedColumns(List<ColumnInfo> allColumns, TaskConfig taskConfig) {
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();

        if (columnDto == null) {
            return allColumns;
        }

        if (columnDto.getIsAllColumn() != null && columnDto.getIsAllColumn()) {
            // 如果选择所有列，返回全部
            return allColumns;
        }

        List<String> sourceColumns = columnDto.getSourceColumns();
        if (sourceColumns == null || sourceColumns.isEmpty()) {
            // 如果没有指定列，返回全部
            return allColumns;
        }

        // 根据选择的列名过滤
        return allColumns.stream()
                .filter(column -> sourceColumns.contains(column.getName()))
                .collect(Collectors.toList());
    }

    /**
     * 根据TaskConfig构建JobDatasource对象
     */
    private JobDatasource buildJobDatasource(TaskConfig.DataSourceInfo dataSourceInfo, String tableName) {
        JobDatasource jobDatasource = new JobDatasource();

        jobDatasource.setDatasourceType(dataSourceInfo.getDsType());
        jobDatasource.setJdbcUsername(dataSourceInfo.getDsUser());
        jobDatasource.setJdbcPassword(decryptPassword(dataSourceInfo.getEncryptPwd(), dataSourceInfo.getPwdKey()));

        String jdbcUrl = buildJdbcUrl(dataSourceInfo);
        jobDatasource.setJdbcUrl(jdbcUrl);

        String driverClass = getDriverClass(dataSourceInfo.getDsType());
        jobDatasource.setJdbcDriverClass(driverClass);

        jobDatasource.setJdbcHostName(dataSourceInfo.getDbHost());
        jobDatasource.setJdbcPort(Integer.valueOf(dataSourceInfo.getDbPort()));
        jobDatasource.setDatabaseName(dataSourceInfo.getDbName());

        return jobDatasource;
    }

    /**
     * 解密密码
     */
    private String decryptPassword(String encryptedPassword, String key) {
        try {
            if (StringUtils.isBlank(encryptedPassword) || StringUtils.isBlank(key)) {
                return encryptedPassword;
            }
            AESUtils aesUtils = new AESUtils();
            return aesUtils.decrypt(encryptedPassword, key);
        } catch (Exception e) {
            log.error("密码解密失败", e);
            return encryptedPassword;
        }
    }

    /**
     * 构建JDBC URL
     */
    private String buildJdbcUrl(TaskConfig.DataSourceInfo dataSource) {
        if (dataSource.getDsUrl() != null && !dataSource.getDsUrl().isEmpty()) {
            return dataSource.getDsUrl();
        }

        String dsType = dataSource.getDsType().toLowerCase();
        String host = dataSource.getDbHost();
        String port = dataSource.getDbPort();
        String dbName = dataSource.getDbName();

        switch (dsType) {
            case "mysql":
            case "oceanbase":
            case "starrocks":
            case "drds":
                return String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai",
                        host, port, dbName);
            case "postgresql":
                return String.format("jdbc:postgresql://%s:%s/%s", host, port, dbName);
            case "oracle":
                return String.format("jdbc:oracle:thin:@%s:%s:%s", host, port, dbName);
            case "sqlserver":
                return String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s", host, port, dbName);
            case "mongodb":
                return String.format("mongodb://%s:%s/%s", host, port, dbName);
            case "kingbase":
                return String.format("jdbc:kingbase8://%s:%s/%s", host, port, dbName);
            case "clickhouse":
                return String.format("jdbc:clickhouse://%s:%s/%s", host, port, dbName);
            case "hive":
                return String.format("jdbc:hive2://%s:%s/%s", host, port, dbName);
            case "impala":
                return String.format("jdbc:impala://%s:%s/%s", host, port, dbName);
            case "doris":
                return String.format("jdbc:mysql://%s:%s/%s", host, port, dbName);
            case "hdfs":
                return String.format("hdfs://%s:%s", host, port);
            default:
                throw new IllegalArgumentException("不支持的数据库类型: " + dsType);
        }
    }

    /**
     * 获取数据库驱动类
     */
    private String getDriverClass(String dsType) {
        switch (dsType.toLowerCase()) {
            case "mysql":
            case "oceanbase":
            case "starrocks":
            case "drds":
            case "doris":
                return "com.mysql.cj.jdbc.Driver";
            case "postgresql":
                return "org.postgresql.Driver";
            case "oracle":
                return "oracle.jdbc.driver.OracleDriver";
            case "sqlserver":
                return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            case "kingbase":
                return "com.kingbase8.Driver";
            case "clickhouse":
                return "ru.yandex.clickhouse.ClickHouseDriver";
            case "hive":
                return "org.apache.hive.jdbc.HiveDriver";
            case "impala":
                return "com.cloudera.impala.jdbc.Driver";
            case "mongodb":
                return "mongodb.jdbc.MongoDriver";
            case "hdfs":
            case "hbase":
                return null;
            default:
                throw new IllegalArgumentException("不支持的数据库类型: " + dsType);
        }
    }

    /**
     * 列构建结果
     */
    @Data
    @AllArgsConstructor
    public static class ColumnResult {
        private String hiveColumn;  // 用于 writer
        private String dbColumn;     // 用于 reader
    }
}
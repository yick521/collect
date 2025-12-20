package com.zhugeio.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class ColumnBuilder {

    /**
     * 构建列配置字符串
     * @param taskConfig 任务配置
     * @param columns 列信息列表
     * @return ColumnResult 包含hiveColumn和dbColumn的结果
     */
    public ColumnResult buildColumns(TaskConfig taskConfig, List<ColumnInfo> columns) {
        String sourceDbType = taskConfig.getSourceDb().getDsType();

        // 根据数据库类型构建列字符串
        switch (sourceDbType.toLowerCase()) {
            case "mysql":
            case "sqlserver":
            case "oceanbase":
            case "starrocks":
                return buildMysqlTypeColumns(columns);

            case "postgresql":
            case "drds":
                return buildPostgresqlTypeColumns(columns);

            case "mongodb":
                return buildMongodbTypeColumns(columns);

            case "hive":
            case "impala":
                return buildHiveTypeColumns(columns);

            case "oracle":
            case "kingbase":
                return buildOracleTypeColumns(columns);

            case "excel":
                return buildExcelTypeColumns(columns);

            case "hbase":
                return buildHbaseTypeColumns(columns);

            default:
                throw new IllegalArgumentException("不支持的数据库类型: " + sourceDbType);
        }
    }

    /**
     * MySQL类型数据库的列构建
     */
    private ColumnResult buildMysqlTypeColumns(List<ColumnInfo> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return new ColumnResult("", "");
        }

        StringBuilder dbColumnSB = new StringBuilder();
        StringBuilder hiveColumnSB = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo column = columns.get(i);
            if (i == columns.size() - 1) {
                dbColumnSB.append("\"`").append(column.getName()).append("`\"");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"}");
            } else {
                dbColumnSB.append("\"`").append(column.getName()).append("`\",");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"},");
            }
        }

        return new ColumnResult(hiveColumnSB.toString(), dbColumnSB.toString());
    }

    /**
     * PostgreSQL类型数据库的列构建
     */
    private ColumnResult buildPostgresqlTypeColumns(List<ColumnInfo> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return new ColumnResult("", "");
        }

        StringBuilder dbColumnSB = new StringBuilder();
        StringBuilder hiveColumnSB = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo column = columns.get(i);
            if (i == columns.size() - 1) {
                dbColumnSB.append("\"").append(column.getName()).append("\"");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"}");
            } else {
                dbColumnSB.append("\"").append(column.getName()).append("\",");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"},");
            }
        }

        return new ColumnResult(hiveColumnSB.toString(), dbColumnSB.toString());
    }

    /**
     * MongoDB类型数据库的列构建
     */
    private ColumnResult buildMongodbTypeColumns(List<ColumnInfo> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return new ColumnResult("", "");
        }

        StringBuilder dbColumnSB = new StringBuilder();
        StringBuilder hiveColumnSB = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo column = columns.get(i);
            if (i == columns.size() - 1) {
                dbColumnSB.append("{\"name\":\"").append(column.getName()).append("\",\"type\":\"").append(column.getType()).append("\"}");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"}");
            } else {
                dbColumnSB.append("{\"name\":\"").append(column.getName()).append("\",\"type\":\"").append(column.getType()).append("\"},");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"},");
            }
        }

        return new ColumnResult(hiveColumnSB.toString(), dbColumnSB.toString());
    }

    /**
     * Hive/Impala类型数据库的列构建
     */
    private ColumnResult buildHiveTypeColumns(List<ColumnInfo> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return new ColumnResult("", "");
        }

        StringBuilder dbColumnSB = new StringBuilder();
        StringBuilder hiveColumnSB = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo column = columns.get(i);
            if (i == columns.size() - 1) {
                dbColumnSB.append("{\"index\":").append(i).append(",\"type\":\"String\"}");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"}");
            } else {
                dbColumnSB.append("{\"index\":").append(i).append(",\"type\":\"String\"},");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"},");
            }
        }

        return new ColumnResult(hiveColumnSB.toString(), dbColumnSB.toString());
    }

    /**
     * Oracle/KingBase类型数据库的列构建
     */
    private ColumnResult buildOracleTypeColumns(List<ColumnInfo> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return new ColumnResult("", "");
        }

        StringBuilder dbColumnSB = new StringBuilder();
        StringBuilder hiveColumnSB = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo column = columns.get(i);
            if (i == columns.size() - 1) {
                dbColumnSB.append("\"").append(column.getName()).append("\"");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"}");
            } else {
                dbColumnSB.append("\"").append(column.getName()).append("\",");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"},");
            }
        }

        return new ColumnResult(hiveColumnSB.toString(), dbColumnSB.toString());
    }

    /**
     * Excel类型的列构建
     */
    private ColumnResult buildExcelTypeColumns(List<ColumnInfo> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return new ColumnResult("", "");
        }

        StringBuilder dbColumnSB = new StringBuilder();
        StringBuilder hiveColumnSB = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo column = columns.get(i);
            if (i == columns.size() - 1) {
                dbColumnSB.append("{\"index\":").append(i).append(",\"type\":\"").append(column.getType()).append("\"}");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"}");
            } else {
                dbColumnSB.append("{\"index\":").append(i).append(",\"type\":\"").append(column.getType()).append("\"},");
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"},");
            }
        }

        return new ColumnResult(hiveColumnSB.toString(), dbColumnSB.toString());
    }

    /**
     * HBase类型的列构建
     */
    private ColumnResult buildHbaseTypeColumns(List<ColumnInfo> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return new ColumnResult("", "");
        }

        StringBuilder hiveColumnSB = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo column = columns.get(i);
            if (i == columns.size() - 1) {
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"}");
            } else {
                hiveColumnSB.append("{\"name\":\"`").append(column.getName()).append("`\",\"type\":\"").append(column.getHiveType()).append("\"},");
            }
        }

        return new ColumnResult(hiveColumnSB.toString(), "");
    }

    /**
     * 列构建结果
     */
    @Data
    @AllArgsConstructor
    public static class ColumnResult {
        private String hiveColumn;
        private String dbColumn;
    }
}


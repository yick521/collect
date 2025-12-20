package com.zhugeio.meta;


import com.zhugeio.model.ColumnInfo;

import java.util.List;

/**
 * @desc 基础数据库元
 * @Author Liujh
 * @Date 2022/7/18 14:47
 */
public abstract class BaseDatabaseMeta implements DatabaseInterface {


    @Override
    public String getSQLQueryTables() {
        return null;
    }

    @Override
    public String getSQLQueryTables(String taleName) {
        return null;
    }

    @Override
    public String getSQLQueryFields(String tableName) {
        return "SELECT * FROM " + tableName + " limit 1";
    }

    @Override
    public String showFullColumns(String tableName) {
        return "show full columns from " + tableName;
    }

    @Override
    public String getSQLQueryComment(String schemaName, String tableName, String columnName) {
        return null;
    }

    @Override
    public String addColumns(String tableName, String columnName, String type, String comment) {
        return String.format("alter table `%s` add column %s %s comment '%s'", tableName, columnName, type, comment);
    }


    @Override
    public String deleteTable(String tableName) {
        return "drop table " + tableName;
    }

    @Override
    public String createTable(List<ColumnInfo> columnInfos, String tableName) {
        return null;
    }

    public String createTable(List<ColumnInfo> columnInfos, String tableName, int type) {
        return null;
    }
}

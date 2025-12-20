package com.zhugeio.meta;


import com.zhugeio.model.ColumnInfo;

import java.util.List;

/**
 * @desc sql语句父类定义接口
 * @Author Liujh
 * @Date 2022/7/18 14:47
 */
public interface DatabaseInterface {


    /**
     * 获取所有表名的sql
     *
     * @return
     */
    String getSQLQueryTables();

    String getSQLQueryTables(String tableName);

    String getSQLQueryFields(String tableName);

    /**
     * 显示完整的列
     * 获取完整的数据
     *
     * @param tableName 表名
     * @return {@link String}
     */
    String showFullColumns(String tableName);


    /**
     * 获取表和字段注释的sql语句
     *
     * @return The SQL to launch.
     */
    String getSQLQueryComment(String schemaName, String tableName, String columnName);

    /**
     * 添加一列
     *
     * @param tableName  表名
     * @param columnName 列名
     * @param type       类型
     * @param comment    注释
     * @return {@link String}
     */
    String addColumns(String tableName, String columnName, String type, String comment);


    /**
     * 删除表
     *
     * @param tableName 表名
     * @return
     */
    String deleteTable(String tableName);

    String createTable(List<ColumnInfo> columnInfos, String tableName);

}

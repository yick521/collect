package com.zhugeio.utils;


import com.alibaba.druid.util.JdbcUtils;
import com.google.common.collect.Lists;
import com.zhugeio.model.ColumnInfo;
import com.zhugeio.model.Constant;
import com.zhugeio.model.JobDatasource;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * @desc
 * @Author Liujh
 * @Date 2022/7/20 11:33
 */

public class ImpalaJdbcTool extends BaseQueryTool implements QueryToolInterface {
    public ImpalaJdbcTool(JobDatasource jobDatasource) throws SQLException {
        super(jobDatasource);
    }

    public boolean creatTable(List<ColumnInfo> columnInfoList, String tableName) {
        return super.executeSql(sqlBuilder.createTable(columnInfoList, tableName));
    }

    public boolean deleteTable(String tableName) {
        return super.executeSql(sqlBuilder.deleteTable(tableName));
    }

    @Override
    public List<List<String>> executeQueryTableInfo(String tableName) {
        List<List<String>> result = new ArrayList<>();
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        String sql = "select  * from " + tableName + " limit 10 ; ";
        List<String> strings = checkTableName(tableName);
        if (CollectionUtils.isEmpty(strings)) {
            logger.info("表不存在:【{}】", tableName);
            return result;
        }
        try {
            statement = connection.prepareStatement(sql);
            resultSet = statement.executeQuery();
            List<String> column = new ArrayList<>();
            ResultSetMetaData rsMeta = resultSet.getMetaData();
            for (int i = 0; i < rsMeta.getColumnCount(); i++) {
                column.add(rsMeta.getColumnName(i + 1));
            }
            result.add(column);

            while (resultSet.next()) {
                List<String> rows = new ArrayList<>();
                for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                    String columnTypeName = rsMeta.getColumnTypeName(i + 1);
                    //替换缺失值节点维护的续虚拟主键
                    if (Objects.equals(columnTypeName, "zg_row_id")) {
                        continue;
                    }
                    if (Constant.HiveType.TIMESTAMP.equalsIgnoreCase(columnTypeName)) {
                        Timestamp timestamp = resultSet.getTimestamp(i + 1);
                        if (null != timestamp) {
                            rows.add(timestamp.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        } else {
                            rows.add("-");
                        }
                        continue;
                    }else if (Constant.HiveType.DOUBLE.equalsIgnoreCase(columnTypeName)){
                        double aDouble = resultSet.getDouble(i + 1);
                        String formattedValue = String.format("%.2f", aDouble);
                        rows.add(formattedValue);
                        continue;
                    }
                    String value = resultSet.getString(i + 1);
                    if (StringUtils.isBlank(value)) {
                        value = "-";
                    }
                    rows.add(value);
                }
                result.add(rows);
            }
        } catch (SQLException e) {
            logger.error("查询sql异常", e.getLocalizedMessage());
            throw new RuntimeException(e.getLocalizedMessage());
        } finally {
            JdbcUtils.close(resultSet);
            JdbcUtils.close(statement);
        }


        return result;
    }


    public List<String> checkTableName(String tableName) {
        String sqlQueryTables = sqlBuilder.getSQLQueryTables(tableName);
        logger.info(sqlQueryTables);

        List<String> tables = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            //获取sql
            rs = stmt.executeQuery(sqlQueryTables);
            while (rs.next()) {
                String s = rs.getString(1);
                tables.add(s);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getLocalizedMessage());
        } finally {
            JdbcUtils.close(stmt);
            JdbcUtils.close(rs);
        }
        return tables;
    }

    @Override
    public String checkLinkStatus() {
        String sqlQueryTables = "select 1";
        logger.info(sqlQueryTables);
        String result = "";
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            //获取sql
            rs = stmt.executeQuery(sqlQueryTables);
            while (rs.next()) {
                result = rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getLocalizedMessage());
        } finally {
            JdbcUtils.close(stmt);
            JdbcUtils.close(rs);
        }
        return result;
    }

    @Override
    public List<ColumnInfo> getColumns(String tableName) {
        List<ColumnInfo> fullColumn = Lists.newArrayList();
        Statement statement = null;
        ResultSet resultSet = null;
        //获取指定表的所有字段
        try {
            //获取查询指定表所有字段的sql语句
            String querySql = sqlBuilder.getSQLQueryFields(tableName);
            logger.info("querySql: {}", querySql);
            //获取所有字段
            statement = connection.createStatement();
            resultSet = statement.executeQuery(querySql);
            while (resultSet.next()) {
                ColumnInfo columnInfo = new ColumnInfo();
                for (int i = 0; i < 3; i++) {
                    if (i + 1 == 1) {
                        columnInfo.setName(resultSet.getString(i + 1));
                    }
                    if (i + 1 == 2) {
                        columnInfo.setType(resultSet.getString(i + 1));
                    }
                    if (i + 1 == 3) {
                        columnInfo.setComment(StringUtils.isNotBlank(resultSet.getString(i + 1)) ? resultSet.getString(i + 1) : null);
                    }
                }

                fullColumn.add(columnInfo);
            }
            return fullColumn;

        } catch (SQLException e) {
            logger.error("[getColumns Exception] --> "
                    + "the exception msg is:" + e.getMessage());
            throw new RuntimeException(e.getLocalizedMessage());
        } finally {
            JdbcUtils.close(resultSet);
            JdbcUtils.close(statement);
        }

    }

    @Override
    public List<String> getTables() {
        List<String> tables = Lists.newArrayList();
        Statement statement = null;
        ResultSet resultSet = null;
        //获取指定表的所有字段
        try {
            //获取查询指定表所有字段的sql语句
            String querySql = "show tables";
            logger.info("querySql: {}", querySql);
            //获取所有字段
            statement = connection.createStatement();
            resultSet = statement.executeQuery(querySql);
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                tables.add(name);
            }

        } catch (SQLException e) {
            logger.error("[getColumns Exception] --> "
                    + "the exception msg is:" + e.getMessage());
            throw new RuntimeException(e.getLocalizedMessage());
        } finally {
            JdbcUtils.close(resultSet);
            JdbcUtils.close(statement);
        }
        return tables;
    }
}

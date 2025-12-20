package com.zhugeio.utils;

import com.zhugeio.model.ColumnInfo;
import com.zhugeio.model.JobDatasource;

import java.sql.SQLException;
import java.util.List;

/**
 * mysql数据库使用的查询工具
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName MySQLQueryTool
 * @Version 1.0
 * @since 2019/7/18 9:31
 */
public class DorisQueryTool extends BaseQueryTool implements QueryToolInterface {

    /**
     * 用于获取查询语句
     */

    public DorisQueryTool(JobDatasource jobDatasource) throws SQLException {
        super(jobDatasource);
    }

    public boolean creatTable(List<ColumnInfo> columnInfoList, String tableName) {

        return super.executeSql(sqlBuilder.createTable(columnInfoList, tableName));
    }

}

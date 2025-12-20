package com.zhugeio.utils;


import com.zhugeio.model.ColumnInfo;

import java.util.List;

/**
 * @author EDY
 */
public interface QueryToolInterface {

	/**
	 * 获取当前schema下的所有表
	 *
	 * @return
	 */
	List<String> getTables();

	/**
	 * 表是否存在
	 * @param tableName
	 * @return
	 */
	Boolean tableExists(String tableName);
	/**
	 * 根据表名获取表结构
	 * @param tableName
	 * @return
	 */
	List<ColumnInfo> getColumns(String tableName);

	/**
	 * 执行sql语句
	 * @param sql
	 * @return
	 */
	boolean executeSql(String sql);

	/**
	 * 获取表结构和表数据
	 * @param tableName
	 * @return
	 */
	List<List<String>> executeQueryTableInfo(String tableName);
}

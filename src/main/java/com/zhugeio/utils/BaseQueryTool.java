package com.zhugeio.utils;

import com.alibaba.druid.util.JdbcUtils;
import com.google.common.collect.Lists;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.meta.DatabaseMetaFactory;
import com.zhugeio.model.ColumnInfo;
import com.zhugeio.model.Constant;
import com.zhugeio.meta.DatabaseInterface;
import com.zhugeio.model.DasColumn;
import com.zhugeio.model.JobDatasource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * @desc
 * @Author Liujh
 * @Date 2022/7/18 14:50
 */
@Slf4j
public class BaseQueryTool implements QueryToolInterface {


	protected static final Logger logger = LoggerFactory.getLogger(BaseQueryTool.class);
	/**
	 * 用于获取查询语句
	 */
	protected DatabaseInterface sqlBuilder;

	protected DataSource datasource;

	protected Connection connection;

	protected String dataType;
	/**
	 * 当前数据库名
	 */
	protected String currentSchema;

	protected String currentDatabase;


	public BaseQueryTool(JobDatasource jobDatasource) throws SQLException {

		getDataSource(jobDatasource);
		sqlBuilder = DatabaseMetaFactory.getByDbType(jobDatasource.getDatasourceType());
		currentSchema = getSchema(jobDatasource.getJdbcUsername());
		currentDatabase = jobDatasource.getDatasourceType();
	}


	@Override
	public List<String> getTables() {
		String sqlQueryTables = sqlBuilder.getSQLQueryTables();
		logger.info(sqlQueryTables);

		List<String> tables = new ArrayList<String>();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.createStatement();
			//获取sql
			rs = stmt.executeQuery(sqlQueryTables);
			while (rs.next()) {
				String tableName = rs.getString(1);
				tables.add(tableName);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getLocalizedMessage());
		} finally {
			try {
				stmt.close();
			} catch (Exception e) {
				logger.debug("close statement error", e);
			}
		}
		return tables;
	}

	@Override
	public Boolean tableExists(String tableName) {
		String sql = String.format("show tables '%s'", tableName);
		logger.info(sql);

		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = connection.createStatement();
			//获取sql
			rs = stmt.executeQuery(sql);
			return rs.next();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getLocalizedMessage());
		} finally {
			try {
				stmt.close();
			} catch (Exception e) {
				logger.debug("close statement error", e);
			}
		}
	}

	@Override
	public List<List<String>> executeQueryTableInfo(String tableName) {
		List<List<String>> result = new ArrayList<>();
		Statement statement = null;
		ResultSet resultSet = null;
		String sql = "select  * from " + tableName + " limit 10 ; ";
		try {
			statement = connection.createStatement();
			resultSet = statement.executeQuery(sql);
//			statement = connection.prepareStatement(sql);
//			resultSet = statement.executeQuery();
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

	/**
	 * 用于执行sql语句
	 * @param sql
	 * @return
	 */
	@Override
	public boolean executeSql(String sql) {

		logger.info("executeSql: {}", sql);
		Statement statement = null;
		try {
			 statement = connection.createStatement();
			 statement.execute(sql) ;
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("executeSql error: {}",e);
			throw new RuntimeException("executeSql error");
		} finally {
			JdbcUtils.close(statement);
			//JdbcUtils.close(connection);
		}
		return true;
	}

	public String checkLinkStatus(){
		return null;
	};

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
			ResultSetMetaData metaData = resultSet.getMetaData();

			List<DasColumn> dasColumns = buildDasColumn(tableName, metaData);
			statement.close();

			//构建 fullColumn
			fullColumn = buildFullColumn(dasColumns,tableName);

		} catch (SQLException e) {
			logger.error("[getColumns Exception] --> "
					+ "the exception msg is:" + e.getMessage());
			throw new RuntimeException(e.getLocalizedMessage());
		}finally {
			JdbcUtils.close(resultSet);
			JdbcUtils.close(statement);
		}
		return fullColumn;
	}

	private List<ColumnInfo> buildFullColumn(List<DasColumn> dasColumns,String tableName) {
		List<ColumnInfo> res = Lists.newArrayList();
		if(Constant.HIVE_TYPE.equalsIgnoreCase(dataType)){
			dasColumns.forEach(e -> {
				ColumnInfo columnInfo = new ColumnInfo();
				if(StringUtils.isNotBlank(e.getColumnName()) ){
					columnInfo.setName(e.getColumnName().replace(tableName+".",""));
				}
				columnInfo.setComment(e.getColumnComment());
				columnInfo.setType(e.getColumnTypeName());
				columnInfo.setIsPrimaryKey(e.isIsprimaryKey());
				// columnInfo.setIsnull(e.getIsNull());
				res.add(columnInfo);
			});
		}else {
			dasColumns.forEach(e -> {
				ColumnInfo columnInfo = new ColumnInfo();
				columnInfo.setName(e.getColumnName());
				columnInfo.setComment(e.getColumnComment());
				columnInfo.setType(e.getColumnTypeName());
				columnInfo.setIsPrimaryKey(e.isIsprimaryKey());
				// columnInfo.setIsnull(e.getIsNull());
				res.add(columnInfo);
			});
		}

		return res;
	}

	//构建DasColumn对象
	private List<DasColumn> buildDasColumn(String tableName, ResultSetMetaData metaData) {
		List<DasColumn> res = Lists.newArrayList();
		try {
			int columnCount = metaData.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				DasColumn dasColumn = new DasColumn();
				dasColumn.setColumnClassName(metaData.getColumnClassName(i));
				dasColumn.setColumnTypeName(metaData.getColumnTypeName(i));
				dasColumn.setColumnName(metaData.getColumnName(i));
				// dasColumn.setIsNull(metaData.isNullable(i));

				res.add(dasColumn);
			}

			Statement statement = connection.createStatement();

			if (currentDatabase.equalsIgnoreCase(Constant.MYSQL_TYPE) || currentDatabase.equalsIgnoreCase(Constant.ORACLE_TYPE)) {
				DatabaseMetaData databaseMetaData = connection.getMetaData();

				ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, null, tableName);

				while (resultSet.next()) {
					String name = resultSet.getString("COLUMN_NAME");
					res.forEach(e -> {
						if (e.getColumnName().equals(name)) {
							e.setIsprimaryKey(true);
						} else {
							e.setIsprimaryKey(false);
						}
					});
				}

				res.forEach(e -> {
					String sqlQueryComment = sqlBuilder.getSQLQueryComment(currentSchema, tableName, e.getColumnName());
					//查询字段注释
					try {
						ResultSet resultSetComment = statement.executeQuery(sqlQueryComment);
						while (resultSetComment.next()) {
							e.setColumnComment(resultSetComment.getString(1));
						}
						JdbcUtils.close(resultSetComment);
					} catch (SQLException e1) {
						logger.error("[buildDasColumn executeQuery Exception] --> "
								+ "the exception msg is:" + e1.getMessage());
					}
				});
			}

			JdbcUtils.close(statement);
		} catch (SQLException e) {
			logger.error("[buildDasColumn Exception] --> "
					+ "the exception msg is:" + e.getMessage());
		}
		return res;
	}

	public void getDataSource(JobDatasource jobDatasource) throws SQLException {
		logger.info(" start connection  {}",jobDatasource);
		//
		//这里使用 hikari 数据源
		HikariDataSource dataSource = new HikariDataSource();
		dataSource.setUsername(jobDatasource.getJdbcUsername());
		dataSource.setPassword(jobDatasource.getJdbcPassword());
		dataSource.setJdbcUrl(jobDatasource.getJdbcUrl());
		dataSource.setDriverClassName(jobDatasource.getJdbcDriverClass());
		dataSource.setMaximumPoolSize(1);
		dataSource.setMinimumIdle(0);
		String testSQL = "SELECT 1";
		if ("hana".equalsIgnoreCase(jobDatasource.getDatasourceType())) {
			testSQL = "SELECT 1 from tables LIMIT 1";
		} else if (Constant.ORACLE_TYPE.equalsIgnoreCase(jobDatasource.getDatasourceType())) {
			testSQL = "select 1 from dual";
		}
		// else if(Contains.IMPALA_TYPE.equalsIgnoreCase(jobDatasource.getDatasourceType())){
		// 	testSQL = "select now()";
		// 	dataSource.setValidationTimeout(50000);
		// }
		dataSource.setConnectionTestQuery(testSQL);
		dataSource.setConnectionTimeout(60000);
		this.dataType = jobDatasource.getDatasourceType();
		this.connection = dataSource.getConnection();

		logger.info("connection init success");
	}


	//根据connection获取schema
	public String getSchema(String jdbcUsername) {
		String res = null;
		try {
			res = connection.getCatalog();
		} catch (SQLException e) {
			try {
				res = connection.getSchema();
			} catch (SQLException e1) {
				logger.error("[SQLException getSchema Exception] --> "
						+ "the exception msg is:" + e1.getMessage());
			}
			logger.error("[getSchema Exception] --> "
					+ "the exception msg is:" + e.getMessage());
		}
		// 如果res是null，则将用户名当作 schema
		if (StringUtils.isBlank(res) && StringUtils.isNotBlank(jdbcUsername)) {
			res = jdbcUsername.toUpperCase();
		}
		return res;
	}

	public void closeCon(){
		JdbcUtils.close(connection);
	}



	public boolean executeSql(BaseQueryTool impala,String sql) {
		logger.info("executeSql: {}", sql);
		try {
			impala.executeSql(sql) ;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getLocalizedMessage());
		} finally {
			impala.closeCon();
		}
		return true;
	}
}

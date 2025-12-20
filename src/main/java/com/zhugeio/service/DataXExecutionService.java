package com.zhugeio.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.zhugeio.config.SyncTaskConfig;
import com.zhugeio.enums.IncrementTypeEnum;
import com.zhugeio.model.*;
import com.zhugeio.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static com.zhugeio.model.Constant.*;

/**
 * DataX执行服务
 * 应用启动后自动执行同步任务
 */
@Slf4j
@Service
public class DataXExecutionService implements CommandLineRunner {

    @Autowired
    private TaskConfigService taskConfigService;

    @Autowired
    private TargetDataSourceService targetDataSourceService;

    @Autowired
    private AESUtils aesUtils;

    @Autowired
    private SyncTaskConfig syncTaskConfig;


    /**
     * 应用启动完成后自动执行
     */
    @Override
    public void run(String... args) throws Exception {
        log.info("========== DataX同步服务启动完成 ==========");

        if (!taskConfigService.isConfigLoaded()) {
            log.warn("任务配置未加载，跳过自动执行");
            return;
        }

        TaskConfig taskConfig = taskConfigService.getTaskConfig();
        log.info("开始执行数据同步任务: {}", taskConfig.getName());

        try {
            // 执行同步任务
            boolean success = executeDataXTask(taskConfig);

            if (success) {
                log.info("数据同步任务执行成功: {}", taskConfig.getName());
            } else {
                log.error("数据同步任务执行失败: {}", taskConfig.getName());
                System.exit(1); // 任务失败时退出
            }

        } catch (Exception e) {
            log.error("执行数据同步任务异常: {}, 错误: {}", taskConfig.getName(), e.getMessage(), e);
            System.exit(1); // 异常时退出
        }

        log.info("========== DataX同步服务执行完成 ==========");
        System.exit(0); // 任务完成后正常退出
    }

    /**
     * 执行DataX同步任务
     */
    public boolean executeDataXTask(TaskConfig taskConfig) throws Exception {
        log.info("构建DataX JSON配置");

        // 1. 检查并创建目标表
        if (!taskConfig.getSourceDb().getDsType().toLowerCase().equals(LOCAL_TYPE) &&
                !taskConfig.getSourceDb().getDsType().toLowerCase().equals(FTP_TYPE) &&
                !taskConfig.getSourceDb().getDsType().toLowerCase().equals(EXCEL_TYPE) &&
                !taskConfig.getSourceDb().getDsType().toLowerCase().equals(HBASE_DB_TYPE) &&
                !taskConfig.getSourceDb().getDsType().toLowerCase().equals(HDFS_TYPE)) {
            ensureTargetTableExists(taskConfig);
        }


        //  如果是全量同步，清空目标表
        if (isFullSync(taskConfig)) {
            targetDataSourceService.clearTargetTable(taskConfig);
        }

        // 2. 构建DataX JSON
        String dataxJson = buildDataXJson(taskConfig);

        // 3. 执行DataX
        boolean result = executeDataX(dataxJson, taskConfig.getName());

        targetDataSourceService.refreshTargetTable(taskConfig);
        return result;
    }

    // 添加判断是否为全量同步的方法
    private boolean isFullSync(TaskConfig taskConfig) {
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();
        if (columnDto != null) {
            String incrementType = columnDto.getIncrementType();
            return "ALL".equals(incrementType) || StringUtils.isBlank(incrementType);
        }
        return true; // 默认为全量同步
    }



    /**
     * 确保目标表存在，如果不存在则自动创建
     */
    private void ensureTargetTableExists(TaskConfig taskConfig) throws Exception {
        String targetTable = taskConfig.getTargetTable();
        TaskConfig.DataSourceInfo sourceDb = taskConfig.getSourceDb();


        try {
            TaskColumnBuilder columnBuilder = new TaskColumnBuilder();
            // 第一步：获取表结构信息
            List<ColumnInfo> allColumns = columnBuilder.getTableColumns(taskConfig);

            // 第二步：根据配置过滤字段
            List<ColumnInfo> selectedColumns = columnBuilder.filterSelectedColumns(allColumns, taskConfig);

            JobDatasource jobDatasource = new JobDatasource();

            jobDatasource.setJdbcDriverClass(syncTaskConfig.getTargetDataSource().getDriverClassName());
            jobDatasource.setJdbcPassword(syncTaskConfig.getTargetDataSource().getPassword());
            jobDatasource.setJdbcUrl(syncTaskConfig.getTargetDataSource().getJdbcUrl());
            jobDatasource.setJdbcUsername(syncTaskConfig.getTargetDataSource().getUsername());

            //建立hive表
            if (syncTaskConfig.getTargetDataSource().getDbtype() == 2) {
                //建立hive表
                jobDatasource.setDatasourceType(Constant.DORIS_TYPE);
                BaseQueryTool byDbType = QueryToolFactory.getByDbType(jobDatasource);
                TypeConvertUtils.typeToDoris(sourceDb.getDsType(), selectedColumns);
                DorisQueryTool doris = (DorisQueryTool) byDbType;
                doris.creatTable(selectedColumns, targetTable);
                doris.closeCon();
            } else {
                //建立hive表
                jobDatasource.setDatasourceType(Constant.IMPALA_TYPE);
                BaseQueryTool byDbType = QueryToolFactory.getByDbType(jobDatasource);
                TypeConvertUtils.typeToHive(sourceDb.getDsType(), selectedColumns);
                ImpalaJdbcTool impala = (ImpalaJdbcTool) byDbType;
                impala.creatTable(selectedColumns, targetTable);
                impala.closeCon();
            }
        } catch (Exception e) {
            log.error("检查/创建目标表失败: {}, 错误: {}", targetTable, e.getMessage(), e);
            // 不抛出异常，让程序继续执行，由DataX来处理表不存在的情况
            log.warn("目标表检查失败，继续执行DataX同步任务，由DataX处理可能的表不存在问题");
        }
    }




    /**
     * 构建DataX JSON配置
     */
    private String buildDataXJson(TaskConfig taskConfig) throws Exception {
        JSONObject job = new JSONObject();
        JSONObject setting = new JSONObject();

        // 设置基本参数
        JSONObject speed = new JSONObject();
        speed.put("channel", 1);
        JSONObject errorLimit = new JSONObject();
        errorLimit.put("record", 0);
        errorLimit.put("percentage", 0.02);
        setting.put("speed", speed);
        setting.put("errorLimit", errorLimit);

        // 构建Reader
        JSONObject reader = buildReader(taskConfig);

        // 构建Writer
        JSONObject writer = buildWriter(taskConfig);

        // 组装内容
        JSONObject content = new JSONObject();
        content.put("reader", reader);
        content.put("writer", writer);

        job.put("setting", setting);
        job.put("content", new Object[]{content});

        JSONObject result = new JSONObject();
        result.put("job", job);

        String dataxJson = JSON.toJSONString(result, true);

        return dataxJson;
    }

    /**
     * 构建Reader配置
     */
    private JSONObject buildReader(TaskConfig taskConfig) throws Exception {
        TaskConfig.DataSourceInfo sourceDb = taskConfig.getSourceDb();
        String dsType = sourceDb.getDsType().toLowerCase();

        JSONObject reader = new JSONObject();
        JSONObject parameter = new JSONObject();

        // 设置Reader名称
        String readerName = getReaderName(dsType, taskConfig);
        reader.put("name", readerName);

        // 根据数据源类型构建不同的Reader配置
        if (isRelationalDatabase(dsType)) {
            // 关系型数据库配置
            buildRelationalDatabaseReader(parameter, taskConfig);
        } else if (isHdfsBasedSource(dsType)) {
            // HDFS类型数据源配置（Hive/Impala）
            buildHdfsReader(parameter, taskConfig);
        } else if (Constant.MONGO_DB_TYPE.equals(dsType)) {
            // MongoDB配置
            buildMongoDBReader(parameter, taskConfig);
        } else if (Constant.HBASE_DB_TYPE.equals(dsType)) {
            // HBase配置
            buildHBaseReader(parameter, taskConfig);
        } else if (LOCAL_TYPE.equals(dsType)) {
            // 本地文件配置
            buildLocalFileReader(parameter, taskConfig);
        } else if (FTP_TYPE.equals(dsType)) {
            // FTP文件配置
            buildFtpReader(parameter, taskConfig);
        } else if (Constant.HDFS_TYPE.equals(dsType)) {
            // HDFS文件配置
            buildHdfsFileReader(parameter, taskConfig);
        }

        reader.put("parameter", parameter);
        return reader;
    }

    /**
     * 构建关系型数据库Reader配置
     */
    private void buildRelationalDatabaseReader(JSONObject parameter, TaskConfig taskConfig) throws Exception {
        TaskConfig.DataSourceInfo sourceDb = taskConfig.getSourceDb();
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();
        String sourceTable = taskConfig.getSourceTable();
        String dsType = sourceDb.getDsType().toLowerCase();

        // 解密密码
        String password = decryptPassword(sourceDb.getEncryptPwd(), sourceDb.getPwdKey());

        // 基本连接参数
        parameter.put("username", sourceDb.getDsUser());
        parameter.put("password", password);

        // 列信息
        List<String> sourceColumns = columnDto.getSourceColumns();
        List<String> columns = new ArrayList<>();
        for (String columnName : sourceColumns) {
            columns.add(columnName);
        }
        parameter.put("column", columns);

        // 处理增量同步和WHERE条件
        String incrementType = columnDto.getIncrementType();
        String splitPk = columnDto.getSplitPk();

        // 分片键（仅全量同步时设置）
        if ("ALL".equals(incrementType) && StringUtils.isNotBlank(splitPk)) {
            parameter.put("splitPk", splitPk);
            log.info("设置分片键: {}", splitPk);
        }

        // WHERE条件（仅关系型数据库支持增量同步）
        String whereCondition = buildWhereConditionForRelationalDB(taskConfig);
        if (StringUtils.isNotBlank(whereCondition)) {
            parameter.put("where", whereCondition);
            log.info("设置WHERE条件: {}", whereCondition);
        }

        // 连接信息
        JSONObject connection = new JSONObject();
        connection.put("table", new String[]{sourceTable});
        connection.put("jdbcUrl", new String[]{sourceDb.getDsUrl()});
        parameter.put("connection", new Object[]{connection});
    }

    /**
     * 构建HDFS Reader配置（Hive/Impala）
     */
    private void buildHdfsReader(JSONObject parameter, TaskConfig taskConfig) throws Exception {
        TaskConfig.DataSourceInfo sourceDb = taskConfig.getSourceDb();
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();
        String sourceTable = taskConfig.getSourceTable();
        List<String> sourceColumns = columnDto.getSourceColumns();

        // HDFS路径配置
        String hdfsPath = "/user/hive/warehouse/" + sourceDb.getDbName() + ".db/" + sourceTable;
        parameter.put("path", hdfsPath);
        parameter.put("defaultFS", taskConfig.getSourceDb().getDsUrl());

        // 列信息（HDFS使用索引方式）
        List<JSONObject> columns = new ArrayList<>();
        for (int i = 0; i < sourceColumns.size(); i++) {
            JSONObject column = new JSONObject();
            column.put("index", i);
            column.put("type", "String");
            columns.add(column);
        }
        parameter.put("column", columns);

        // HDFS文件格式配置
        parameter.put("fileType", "text");
        parameter.put("encoding", "UTF-8");
        parameter.put("fieldDelimiter", "\t");
        parameter.put("nullFormat", "\\N");

        log.info("构建HDFS Reader - 路径: {}, 不支持增量同步", hdfsPath);
    }

    /**
     * 构建MongoDB Reader配置
     */
    private void buildMongoDBReader(JSONObject parameter, TaskConfig taskConfig) throws Exception {
        TaskConfig.DataSourceInfo sourceDb = taskConfig.getSourceDb();
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();
        String sourceTable = taskConfig.getSourceTable();
        List<String> sourceColumns = columnDto.getSourceColumns();

        // 解密密码
        String password = decryptPassword(sourceDb.getEncryptPwd(), sourceDb.getPwdKey());

        // MongoDB连接配置
        parameter.put("address", new String[]{sourceDb.getDbHost() + ":" + sourceDb.getDbPort()});
        parameter.put("userName", sourceDb.getDsUser());
        parameter.put("userPassword", password);
        parameter.put("dbName", sourceDb.getDbName());
        parameter.put("collectionName", sourceTable);

        // 列信息
        List<JSONObject> columns = new ArrayList<>();
        for (String columnName : sourceColumns) {
            JSONObject column = new JSONObject();
            column.put("name", columnName);
            column.put("type", "string"); // MongoDB默认类型
            columns.add(column);
        }
        parameter.put("column", columns);

        log.info("构建MongoDB Reader - 集合: {}, 不支持增量同步", sourceTable);
    }

    /**
     * 构建HBase Reader配置
     */
    private void buildHBaseReader(JSONObject parameter, TaskConfig taskConfig) throws Exception {
        TaskConfig.DataSourceInfo sourceDb = taskConfig.getSourceDb();
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();
        String sourceTable = taskConfig.getSourceTable();
        List<String> sourceColumns = columnDto.getSourceColumns();

        // HBase配置
        JSONObject hbaseConfig = new JSONObject();

        // 🔥 修复: 正确处理 ZooKeeper 地址和端口
        String zkQuorum;
        String zkPort = sourceDb.getDbPort(); // 从数据源获取端口

        if (StringUtils.isNotBlank(zkPort)) {
            // 如果 host 已经包含端口，直接使用；否则拼接
            if (sourceDb.getDbHost().contains(":")) {
                zkQuorum = sourceDb.getDbHost();
                // 从 host 中提取端口（如果已包含）
                String[] parts = sourceDb.getDbHost().split(":");
                if (parts.length > 1) {
                    zkPort = parts[1];
                }
            } else {
                zkQuorum = sourceDb.getDbHost() + ":" + zkPort;
            }
        } else {
            zkQuorum = sourceDb.getDbHost();
            zkPort = "2181"; // 默认端口
        }

        hbaseConfig.put("hbase.zookeeper.quorum", zkQuorum);
        hbaseConfig.put("hbase.zookeeper.property.clientPort", zkPort);
        hbaseConfig.put("zookeeper.znode.parent", "/hbase");

        parameter.put("hbaseConfig", hbaseConfig);
        parameter.put("table", sourceTable);
        parameter.put("encoding", "utf-8");
        parameter.put("mode", "normal");

        // 解析列配置 - 支持 JSON 对象格式
        JSONArray columns = parseHBaseReaderColumns(sourceColumns);
        parameter.put("column", columns);

        // Range配置
        JSONObject range = new JSONObject();
        range.put("startRowkey", "");
        range.put("endRowkey", "");
        range.put("isBinaryRowkey", true);
        parameter.put("range", range);

        log.info("构建HBase Reader - 表: {}, ZK: {}, 端口: {}, 列数: {}",
                sourceTable, zkQuorum, zkPort, columns.size());
    }

    /**
     * 解析 HBase Reader 的列配置
     * 支持 JSON 字符串格式: ["{\"name\":\"rowkey\",\"type\":\"string\"}", ...]
     */
    private JSONArray parseHBaseReaderColumns(List<String> sourceColumns) {
        JSONArray columns = new JSONArray();

        if (sourceColumns == null || sourceColumns.isEmpty()) {
            return columns;
        }

        for (String columnStr : sourceColumns) {
            try {
                // 尝试解析为 JSON 对象
                if (columnStr.trim().startsWith("{")) {
                    JSONObject columnObj = JSON.parseObject(columnStr);
                    columns.add(columnObj);
                } else {
                    // 如果不是 JSON 格式，当作普通字符串处理
                    JSONObject column = new JSONObject();
                    column.put("name", columnStr);
                    column.put("type", "string");
                    columns.add(column);
                }
            } catch (Exception e) {
                log.warn("解析 HBase 列配置失败: {}, 使用默认配置", columnStr);
                JSONObject column = new JSONObject();
                column.put("name", columnStr);
                column.put("type", "string");
                columns.add(column);
            }
        }

        log.debug("HBase Reader 列配置: {}", columns);
        return columns;
    }

    /**
     * 构建关系型数据库的WHERE条件（支持增量同步）
     */
    private String buildWhereConditionForRelationalDB(TaskConfig taskConfig) throws Exception {
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();
        String incrementType = columnDto.getIncrementType();
        String incrementColumn = columnDto.getIncrementColumn();
        String userWhere = StringUtils.isBlank(columnDto.getWhere()) ? "" : columnDto.getWhere();
        String targetTable = taskConfig.getTargetTable();
        if (!targetTable.contains(".")) {
            targetTable = taskConfig.getTargetDb().getDbName() + "." + targetTable;
        }

        IncrementTypeEnum incrementTypeEnum = IncrementTypeEnum.fromName(incrementType);


        // 处理增量同步条件
        if (StringUtils.isNotBlank(incrementColumn)) {
            log.info("处理增量同步 - 目标表: {}, 增量字段: {}", targetTable, incrementColumn);
            // 2. 查询增量最大值
            IncrementInfo incrementInfo = targetDataSourceService.queryIncrementMaxValue(targetTable, incrementColumn, incrementTypeEnum == IncrementTypeEnum.ADD_TIME);

            if (!StringUtils.isBlank(incrementInfo.getRealValue())) {
                if (StringUtils.isBlank(userWhere)) {
                    userWhere = incrementColumn + " > " + incrementInfo.getRealValue();
                } else {
                    userWhere = userWhere + " and "  + incrementColumn + " > " + incrementInfo.getRealValue();
                }
            }
        }

        String finalWhere = userWhere;
        if (StringUtils.isNotBlank(finalWhere)) {
            log.info("最终WHERE条件: {}", finalWhere);
        }
        return finalWhere;
    }

    /**
     * 构建Writer配置
     * 直接从目标表获取字段信息
     */
    private JSONObject buildWriter(TaskConfig taskConfig) throws Exception {
        String[] parts = taskConfig.getTargetTable().split("\\.");
        String writerName = parts.length > 1 ? parts[1] : taskConfig.getTargetTable();
        String targetTable = taskConfig.getTargetTable();

        JSONObject writer = new JSONObject();
        JSONObject parameter = new JSONObject();

        // 从目标表获取字段信息
        List<ColumnInfo> targetColumns = getTargetTableColumns(targetTable);

        if (syncTaskConfig.getTargetDataSource().getDbtype() == 1) {
            // HDFS Writer (Impala/Hive)
            writer.put("name", "hdfswriter");

            parameter.put("defaultFS", syncTaskConfig.getHdfsDefaultFS());
            parameter.put("fileType", "text");
            parameter.put("path", "/user/hive/warehouse/ods.db/" + writerName);
            parameter.put("fileName", writerName);
            parameter.put("column", buildHdfsWriterColumns(targetColumns));
            parameter.put("writeMode", "append");
            parameter.put("fieldDelimiter", "\t");

        } else if (syncTaskConfig.getTargetDataSource().getDbtype() == 2) {
            // Doris Writer
            writer.put("name", "doriswriter");

            JSONArray loadUrls = new JSONArray();
            loadUrls.add(syncTaskConfig.getDorisLoadUrl());
            parameter.put("loadUrl", loadUrls);
            parameter.put("username", syncTaskConfig.getTargetDataSource().getUsername());
            parameter.put("password", syncTaskConfig.getTargetDataSource().getPassword());
            parameter.put("column", buildDorisWriterColumns(targetColumns));

            // 连接配置
            JSONArray connections = new JSONArray();
            JSONObject connection = new JSONObject();
            connection.put("selectedDatabase", "ods");
            JSONArray tables = new JSONArray();
            tables.add(targetTable);
            connection.put("table", tables);
            connections.add(connection);
            parameter.put("connection", connections);

        } else if (syncTaskConfig.getTargetDataSource().getDbtype() == 3) {
            // HDFS Writer (quark3)
            writer.put("name", "hdfswriter");

            parameter.put("defaultFS", syncTaskConfig.getHdfsDefaultFS());
            parameter.put("fileType", "text");
            parameter.put("path", "/quark3/user/hive/warehouse/ods.db/hive/" + targetTable);
            parameter.put("fileName", writerName);
            parameter.put("column", buildHdfsWriterColumns(targetColumns));
            parameter.put("writeMode", "append");
            parameter.put("fieldDelimiter", "\t");

        } else {
            // 默认 HDFS Writer
            writer.put("name", "hdfswriter");

            parameter.put("defaultFS", syncTaskConfig.getHdfsDefaultFS());
            parameter.put("fileType", "text");
            parameter.put("path", "/user/hive/warehouse/ods.db/" + writerName);
            parameter.put("fileName", writerName);
            parameter.put("column", buildHdfsWriterColumns(targetColumns));
            parameter.put("writeMode", "append");
            parameter.put("fieldDelimiter", "\t");
        }

        writer.put("parameter", parameter);
        return writer;
    }

    /**
     * 从目标表获取字段信息
     */
    private List<ColumnInfo> getTargetTableColumns(String targetTable) throws Exception {
        JobDatasource targetDatasource = new JobDatasource();
        targetDatasource.setJdbcUrl(syncTaskConfig.getTargetDataSource().getJdbcUrl());
        targetDatasource.setJdbcUsername(syncTaskConfig.getTargetDataSource().getUsername());
        targetDatasource.setJdbcPassword(syncTaskConfig.getTargetDataSource().getPassword());
        targetDatasource.setJdbcDriverClass(syncTaskConfig.getTargetDataSource().getDriverClassName());

        List<ColumnInfo> columns;

        if (syncTaskConfig.getTargetDataSource().getDbtype() == 1) {
            // Impala
            targetDatasource.setDatasourceType("impala");
            ImpalaJdbcTool impala = new ImpalaJdbcTool(targetDatasource);
            columns = impala.getColumns(targetTable);
            impala.closeCon();
        } else if (syncTaskConfig.getTargetDataSource().getDbtype() == 2) {
            // Doris (使用MySQL协议)
            targetDatasource.setDatasourceType("doris");
            DorisQueryTool doris = new DorisQueryTool(targetDatasource);
            columns = doris.getColumns(targetTable);
            doris.closeCon();
        } else {
            throw new IllegalArgumentException("不支持的目标数据库类型: " + syncTaskConfig.getTargetDataSource().getDbtype());
        }

        log.info("从目标表 {} 获取到 {} 个字段", targetTable, columns.size());
        return columns;
    }

    /**
     * 构建Doris Writer的列配置（字符串数组格式，需要反引号）
     */
    private JSONArray buildDorisWriterColumns(List<ColumnInfo> columns) {
        JSONArray columnArray = new JSONArray();
        for (ColumnInfo column : columns) {
            // 🔥 修复：为字段名添加反引号
            columnArray.add("`" + column.getName() + "`");
        }
        log.info("Doris Writer列配置: {}", columnArray);
        return columnArray;
    }

    /**
     * 构建HDFS Writer的列配置（JSON对象数组格式）
     */
    private JSONArray buildHdfsWriterColumns(List<ColumnInfo> columns) {
        JSONArray columnArray = new JSONArray();
        for (ColumnInfo column : columns) {
            JSONObject columnObj = new JSONObject();
            columnObj.put("name", column.getName());
            String type  = "string";
            if (column.getHiveType() != null) {
                type = column.getHiveType();
            } else if(column.getType() != null) {
                type = column.getType();
            }
            columnObj.put("type",  type);
            columnArray.add(columnObj);
        }
        log.info("HDFS Writer列配置: {}", columnArray);
        return columnArray;
    }

    /**
     * 解析文件类型 reader 的列配置
     * 支持两种格式：
     * 1. JSON字符串格式: ["{\"index\":0,\"type\":\"STRING\"}", "{\"index\":1,\"type\":\"STRING\"}"]
     * 2. 普通字符串格式: ["column1", "column2"]（向后兼容）
     */
    private JSONArray parseFileReaderColumns(List<String> sourceColumns) {
        JSONArray columns = new JSONArray();

        if (sourceColumns == null || sourceColumns.isEmpty()) {
            return columns;
        }

        for (String columnStr : sourceColumns) {
            try {
                // 尝试解析为 JSON 对象
                if (columnStr.trim().startsWith("{")) {
                    JSONObject columnObj = JSON.parseObject(columnStr);
                    columns.add(columnObj);
                } else {
                    // 如果不是 JSON 格式，当作普通字符串处理（向后兼容）
                    JSONObject columnObj = new JSONObject();
                    columnObj.put("index", columns.size());
                    columnObj.put("type", "STRING");
                    columns.add(columnObj);
                }
            } catch (Exception e) {
                log.warn("解析列配置失败: {}, 使用默认配置", columnStr);
                JSONObject columnObj = new JSONObject();
                columnObj.put("index", columns.size());
                columnObj.put("type", "STRING");
                columns.add(columnObj);
            }
        }

        return columns;
    }

    /**
     * 构建FTP Reader配置
     */
    private void buildFtpReader(JSONObject parameter, TaskConfig taskConfig) throws Exception {
        TaskConfig.DataSourceInfo sourceDb = taskConfig.getSourceDb();
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();
        String sourceTable = taskConfig.getSourceTable();
        List<String> sourceColumns = columnDto.getSourceColumns();

        // 解密密码
        String password = decryptPassword(sourceDb.getEncryptPwd(), sourceDb.getPwdKey());

        // FTP连接配置
        parameter.put("protocol", "ftp");
        parameter.put("host", sourceDb.getDbHost());
        parameter.put("port", Integer.parseInt(sourceDb.getDbPort()));
        parameter.put("username", sourceDb.getDsUser());
        parameter.put("password", password);
        parameter.put("path", new String[]{sourceDb.getDsUrl() + "/" + sourceTable});

        // 解析列配置 - 支持 JSON 对象格式
        JSONArray columns = parseFileReaderColumns(sourceColumns);
        parameter.put("column", columns);

        // CSV文件配置
        parameter.put("encoding", "UTF-8");
        parameter.put("fieldDelimiter", ",");

        JSONObject csvReaderConfig = new JSONObject();
        csvReaderConfig.put("skipEmptyRecords", true);
        parameter.put("csvReaderConfig", csvReaderConfig);

        log.info("构建FTP Reader - 主机: {}, 文件: {}, 列数: {}", sourceDb.getDbHost(), sourceTable, columns.size());
    }

    /**
     * 构建本地文件Reader配置
     */
    private void buildLocalFileReader(JSONObject parameter, TaskConfig taskConfig) throws Exception {
        TaskConfig.DataSourceInfo sourceDb = taskConfig.getSourceDb();
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();
        String sourceTable = taskConfig.getSourceTable();
        List<String> sourceColumns = columnDto.getSourceColumns();

        // 处理文件路径
        String filePath = sourceDb.getDsUrl() + "/" + sourceTable;

        // 本地文件配置
        parameter.put("encoding", "UTF-8");
        parameter.put("path", new String[]{filePath});

        // 解析列配置 - 支持 JSON 对象格式
        JSONArray columns = parseFileReaderColumns(sourceColumns);
        parameter.put("column", columns);

        parameter.put("fieldDelimiter", ",");

        log.info("构建本地文件Reader - 路径: {}, 列数: {}", filePath, columns.size());
    }

    /**
     * 构建HDFS文件Reader配置
     */
    private void buildHdfsFileReader(JSONObject parameter, TaskConfig taskConfig) throws Exception {
        TaskConfig.DataSourceInfo sourceDb = taskConfig.getSourceDb();
        TaskConfig.ColumnDto columnDto = taskConfig.getColumnDto();
        String sourceTable = taskConfig.getSourceTable();
        List<String> sourceColumns = columnDto.getSourceColumns();

        // 🔥 修复：正确解析 HDFS 路径和地址
        String fullPath = sourceDb.getDsUrl();
        String hdfsPath;
        String defaultFS;

        if (StringUtils.isNotBlank(fullPath) && fullPath.startsWith("hdfs://")) {
            // 情况1: hdfs://10.10.0.112:9000/user/root/test.txt
            int pathStartIndex = fullPath.indexOf('/', 7);
            if (pathStartIndex > 0) {
                defaultFS = fullPath.substring(0, pathStartIndex);
                hdfsPath = fullPath.substring(pathStartIndex);
            } else {
                // 情况2: hdfs://10.10.0.112:8020 (没有路径部分)
                defaultFS = taskConfig.getSourceDb().getDsUrl();
                hdfsPath = buildHdfsFilePath(sourceDb, sourceTable);
            }
        } else if (StringUtils.isNotBlank(fullPath) && fullPath.startsWith("/")) {
            // 情况3: /user/root/testdb/test.txt (纯路径)
            defaultFS = getDefaultFS();
            hdfsPath = fullPath;
        } else {
            // 情况4: 其他情况
            defaultFS = getDefaultFS();
            hdfsPath = buildHdfsFilePath(sourceDb, sourceTable);
        }

        // HDFS文件路径配置
        parameter.put("path", hdfsPath);
        parameter.put("defaultFS", defaultFS);

        // 解析列配置 - 支持 JSON 对象格式
        JSONArray columns = parseFileReaderColumns(sourceColumns);
        parameter.put("column", columns);

        // 文件格式配置
        String fileType = getHdfsFileType(sourceTable);
        parameter.put("fileType", fileType);
        parameter.put("encoding", "UTF-8");

        // 根据文件类型设置不同的分隔符
        if ("text".equals(fileType)) {
            parameter.put("fieldDelimiter", getFieldDelimiter(sourceTable));
            parameter.put("nullFormat", "\\N");
        } else if ("orc".equals(fileType)) {
            // ORC文件不需要分隔符
        } else if ("parquet".equals(fileType)) {
            // Parquet文件不需要分隔符
        }

        log.info("构建HDFS文件Reader - DefaultFS: {}, 路径: {}, 文件类型: {}, 列数: {}",
                defaultFS, hdfsPath, fileType, columns.size());
    }

    /**
     * 构建HDFS文件路径（只返回路径部分，不包含 hdfs:// 前缀）
     */
    private String buildHdfsFilePath(TaskConfig.DataSourceInfo sourceDb, String sourceTable) {
        String dsUrl = sourceDb.getDsUrl();

        // 🔥 如果 URL 包含 hdfs:// 前缀，提取纯路径部分
        if (StringUtils.isNotBlank(dsUrl) && dsUrl.startsWith("hdfs://")) {
            try {
                // 解析 HDFS URL，只提取路径部分
                int pathStartIndex = dsUrl.indexOf('/', 7); // 跳过 "hdfs://"
                if (pathStartIndex > 0) {
                    String purePath = dsUrl.substring(pathStartIndex);
                    log.debug("从 HDFS URL 提取路径: {} -> {}", dsUrl, purePath);
                    return purePath;
                }
            } catch (Exception e) {
                log.warn("解析 HDFS URL 失败: {}", dsUrl, e);
            }
        }

        // 如果 dsUrl 已经是路径格式（以 / 开头）
        if (StringUtils.isNotBlank(dsUrl) && dsUrl.startsWith("/")) {
            return dsUrl;
        }

        // 否则构建标准路径
        String basePath = StringUtils.isNotBlank(dsUrl) ? dsUrl : "/user/data";

        // 如果sourceTable包含路径，直接拼接
        if (StringUtils.isNotBlank(sourceTable)) {
            if (sourceTable.startsWith("/")) {
                return sourceTable;
            } else {
                return basePath + "/" + sourceTable;
            }
        }

        return basePath;
    }

    /**
     * 根据文件名推断文件类型
     */
    private String getHdfsFileType(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return "text";
        }

        String lowerFileName = fileName.toLowerCase();
        if (lowerFileName.endsWith(".orc")) {
            return "orc";
        } else if (lowerFileName.endsWith(".parquet")) {
            return "parquet";
        } else if (lowerFileName.endsWith(".csv") || lowerFileName.endsWith(".txt")) {
            return "text";
        } else {
            // 默认为文本格式
            return "text";
        }
    }

    /**
     * 根据文件类型获取字段分隔符
     */
    private String getFieldDelimiter(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return "\t"; // 默认tab分隔
        }

        String lowerFileName = fileName.toLowerCase();
        if (lowerFileName.endsWith(".csv")) {
            return ",";
        } else if (lowerFileName.endsWith(".tsv")) {
            return "\t";
        } else {
            return "\t"; // 默认tab分隔
        }
    }

    /**
     * 获取文件数据源的具体类型
     */
    private String getFileSourceType(TaskConfig.DataSourceInfo sourceDb) {
        // 这里需要根据实际的数据源配置来判断是FTP还是LOCAL
        // 可以通过URL格式、端口号或其他字段来判断
        if (StringUtils.isNotBlank(sourceDb.getDbHost()) && StringUtils.isNotBlank(sourceDb.getDbPort())) {
            return FTP_TYPE;
        } else {
            return LOCAL_TYPE;
        }
    }

    /**
     * 执行DataX任务
     */
    private boolean executeDataX(String dataxJson, String taskName) throws Exception {
        // 使用当前执行目录创建临时文件
        String currentDir = System.getProperty("user.dir");
        log.info("当前执行目录: {}", currentDir);

        // 创建文件名（避免特殊字符）
        String safeTaskName = taskName.replaceAll("[^a-zA-Z0-9\\u4e00-\\u9fa5]", "_");
        String fileName = "datax_" + safeTaskName + "_" + System.currentTimeMillis() + ".json";
        File tempFile = new File(currentDir, fileName);

        ObjectMapper mapper = new ObjectMapper();

        try {
            // 解析 JSON
            JsonNode rootNode = mapper.readTree(dataxJson);

            // 获取 where 条件节点
            JsonNode readerParams = rootNode.path("job").path("content").get(0).path("reader").path("parameter");

            if (readerParams.has("where")) {
                String whereClause = readerParams.path("where").asText();
                log.info("原始 where 条件: {}", whereClause);

                // 检查是否包含单引号，如果没有则添加
                if (!whereClause.contains("'")) {
                    // 匹配 = 后面的值并添加单引号
                    whereClause = whereClause.replaceAll("=\\s*([A-Za-z0-9]+)", "= '$1'");
                }

                ((ObjectNode) readerParams).put("where", whereClause);
                log.info("修复后的 where 条件: {}", whereClause);
            }

            // 写入文件
            String finalJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);

            try (OutputStreamWriter writer = new OutputStreamWriter(
                    new FileOutputStream(tempFile), StandardCharsets.UTF_8)) {
                writer.write(finalJson);
            }

            log.info("DataX配置文件已写入: {}", tempFile.getAbsolutePath());

        } catch (Exception e) {
            log.error("JSON 处理失败", e);
            // 如果处理失败，尝试直接写入原始内容
            try (OutputStreamWriter writer = new OutputStreamWriter(
                    new FileOutputStream(tempFile), StandardCharsets.UTF_8)) {
                writer.write(dataxJson);
            }
        }

        log.info("DataX配置文件已写入: {}", tempFile.getAbsolutePath());

        // 构建执行命令
        String[] command = buildDataXCommand(tempFile.getAbsolutePath());
        log.info("执行DataX命令: {}", String.join(" ", command));

        // 执行DataX
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true); // 合并stderr到stdout
        Process process = processBuilder.start();

        // 实时读取输出
        boolean hasError = false;

        log.info("==================== DataX执行开始 ====================");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 直接输出DataX日志，不带格式
                log.info(line);

                // 检查关键信息
                if (line.contains("Exception") && !line.contains("WARN")) {
                    hasError = true;
                }
            }
        }

        // 等待执行完成
        int exitCode = process.waitFor();

        log.info("==================== DataX执行结束 ====================");
        log.info("DataX执行完成，退出码: {}", exitCode);

        // 判断执行结果
        boolean success = (exitCode == 0) && !hasError;

        if (success) {
            log.info("DataX任务执行成功！");
        } else {
            log.error("DataX任务执行失败，退出码: {}, 发现错误: {}", exitCode, hasError);
        }

        // 清理临时文件
        if (!syncTaskConfig.isDebug()) {
            boolean deleted = tempFile.delete();
            log.debug("临时文件删除结果: {}", deleted);
        } else {
            log.info("调试模式，保留临时文件: {}", tempFile.getAbsolutePath());
        }

        return success;
    }

    /**
     * 构建DataX执行命令
     */
    private String[] buildDataXCommand(String configFilePath) {
        String dataxPath = syncTaskConfig.getDataxPythonPath();

        // 解析DataX命令
        List<String> commandList = new ArrayList<>();

        if (dataxPath.contains(" ")) {
            // 如果路径包含空格或参数，按空格分割
            String[] parts = dataxPath.split("\\s+");
            for (String part : parts) {
                commandList.add(part);
            }
        } else {
            // 简单路径
            commandList.add(dataxPath);
        }

        // 添加配置文件路径
        commandList.add(configFilePath);

        // 转换为数组
        String[] command = commandList.toArray(new String[0]);

        log.info("构建的DataX命令数组: {}", String.join(" ", command));
        return command;
    }

    /**
     * 解密密码
     */
    private String decryptPassword(String encryptedPassword, String key) {
        try {
            if (StringUtils.isBlank(encryptedPassword) || StringUtils.isBlank(key)) {
                return encryptedPassword;
            }
            String decrypted = aesUtils.decrypt(encryptedPassword, key);
            log.debug("密码解密成功");
            return decrypted;
        } catch (Exception e) {
            log.error("密码解密失败: {}", e.getMessage());
            return encryptedPassword;
        }
    }

    /**
     * 判断是否为关系型数据库
     */
    private boolean isRelationalDatabase(String dsType) {
        return Constant.MYSQL_TYPE.equals(dsType) || Constant.POSTGRE_SQL_TYPE.equals(dsType) ||
                Constant.ORACLE_TYPE.equals(dsType) || Constant.SQL_SERVER_TYPE.equals(dsType) ||
                Constant.KINGBASE_TYPE.equals(dsType) || Constant.OCEAN_BASE_TYPE.equals(dsType) ||
                Constant.STARROCKS_TYPE.equals(dsType) || Constant.DRDS_TYPE.equals(dsType);
    }

    /**
     * 判断是否为HDFS类型数据源
     */
    private boolean isHdfsBasedSource(String dsType) {
        return Constant.HIVE_TYPE.equals(dsType) || Constant.IMPALA_TYPE.equals(dsType);
    }


    /**
     * 获取Reader名称
     */
    private String getReaderName(String dsType, TaskConfig taskConfig) {
        switch (dsType) {
            case Constant.MYSQL_TYPE: return "mysqlreader";
            case Constant.POSTGRE_SQL_TYPE: return "postgresqlreader";
            case Constant.ORACLE_TYPE: return "oraclereader";
            case Constant.SQL_SERVER_TYPE: return "sqlserverreader";
            case Constant.MONGO_DB_TYPE: return "mongodbreader";
            case Constant.HIVE_TYPE: return "hdfsreader";
            case Constant.IMPALA_TYPE: return "hdfsreader";
            case Constant.KINGBASE_TYPE: return "kingbaseesreader";
            case Constant.OCEAN_BASE_TYPE: return "mysqlreader";
            case Constant.STARROCKS_TYPE: return "mysqlreader";
            case Constant.DRDS_TYPE: return "postgresqlreader";
            case Constant.HBASE_DB_TYPE: return "hbase11xreader";
            case EXCEL_TYPE:
            case LOCAL_TYPE:
            case FTP_TYPE:
                return getFileReaderName(taskConfig.getSourceDb());
            case Constant.HDFS_TYPE: return "hdfsreader";
            default:
                log.warn("未知的数据源类型: {}, 使用默认的mysqlreader", dsType);
                return "mysqlreader";
        }
    }

    /**
     * 获取文件类型Reader名称
     */
    private String getFileReaderName(TaskConfig.DataSourceInfo sourceDb) {
        String sourceType = getFileSourceType(sourceDb);
        if (FTP_TYPE.equalsIgnoreCase(sourceType)) {
            return "ftpreader";
        } else {
            return "txtfilereader";
        }
    }

    /**
     * 获取默认HDFS地址
     */
    private String getDefaultFS() {
        // 优先从配置文件中获取
        String hdfsUrl = syncTaskConfig.getHdfsDefaultFS();
        if (StringUtils.isNotBlank(hdfsUrl)) {
            return hdfsUrl;
        }

        // 从环境变量获取
        String envHdfsUrl = System.getProperty("hdfs.defaultFS");
        if (StringUtils.isNotBlank(envHdfsUrl)) {
            return envHdfsUrl;
        }

        // 最后使用默认值
        log.warn("未配置HDFS地址，使用默认值: hdfs://localhost:9000");
        return "hdfs://localhost:9000";
    }

    /**
     * 判断是否需要反引号
     */
    private boolean needBackticks(String dsType) {
        return Constant.MYSQL_TYPE.equals(dsType) || Constant.OCEAN_BASE_TYPE.equals(dsType) ||
                Constant.STARROCKS_TYPE.equals(dsType) || Constant.DORIS_TYPE.equals(dsType);
    }
}
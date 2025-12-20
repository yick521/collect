package com.zhugeio.service;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhugeio.model.TaskConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * 任务配置加载服务
 * 负责在应用启动时加载task.json配置文件
 */
@Slf4j
@Service
public class TaskConfigService {

    @Value("${datax.task.config-file:task.json}")
    private String taskConfigFile;

    private TaskConfig taskConfig;

    /**
     * 应用启动时自动加载任务配置
     */
    @PostConstruct
    public void loadTaskConfig() {
        try {
            log.info("开始加载任务配置文件: {}", taskConfigFile);

            String configContent = readConfigFile();
            if (configContent == null || configContent.trim().isEmpty()) {
                log.warn("任务配置文件为空或不存在，跳过配置加载");
                return;
            }

            // 解析JSON配置
            JSONObject configJson = JSON.parseObject(configContent);
            this.taskConfig = parseTaskConfig(configJson);

            log.info("任务配置加载成功");
            log.info("任务名称: {}", taskConfig.getName());
            log.info("任务类型: {}", taskConfig.getType());
            log.info("调度周期: {}", taskConfig.getCycleType());
            log.info("源数据库: {} - {}", taskConfig.getSourceDb().getDsType(), taskConfig.getSourceDb().getDsName());
            log.info("目标数据库: {} - {}", taskConfig.getTargetDb().getDsType(), taskConfig.getTargetDb().getDsName());
            log.info("源表: {}", taskConfig.getSourceTable());
            log.info("目标表: {}", taskConfig.getTargetTable());

            // 输出增量同步信息
            if (taskConfig.getColumnDto() != null) {
                String incrementType = taskConfig.getColumnDto().getIncrementType();
                String incrementColumn = taskConfig.getColumnDto().getIncrementColumn();
                log.info("同步模式: {}", "ALL".equals(incrementType) ? "全量同步" : "增量同步");
                if (!"ALL".equals(incrementType)) {
                    log.info("增量字段: {}", incrementColumn);
                }
            }

        } catch (Exception e) {
            log.error("加载任务配置失败: {}", e.getMessage(), e);
            throw new RuntimeException("任务配置加载失败", e);
        }
    }

    /**
     * 读取配置文件内容
     */
    private String readConfigFile() throws Exception {
        StringBuilder content = new StringBuilder();

        try {
            // 首先尝试从classpath读取
            Resource resource = new ClassPathResource(taskConfigFile);
            if (resource.exists()) {
                log.info("从classpath读取配置文件: {}", taskConfigFile);
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        content.append(line);
                    }
                }
                return content.toString();
            }
        } catch (Exception e) {
            log.debug("从classpath读取配置文件失败: {}", e.getMessage());
        }

        try {
            // 尝试从文件系统读取
            log.info("从文件系统读取配置文件: {}", taskConfigFile);
            try (BufferedReader reader = new BufferedReader(new FileReader(taskConfigFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    content.append(line);
                }
            }
            return content.toString();
        } catch (Exception e) {
            log.debug("从文件系统读取配置文件失败: {}", e.getMessage());
        }

        log.warn("无法找到配置文件: {}", taskConfigFile);
        return null;
    }

    /**
     * 解析任务配置
     */
    private TaskConfig parseTaskConfig(JSONObject configJson) {
        TaskConfig config = new TaskConfig();

        // 基本信息
        config.setId(configJson.getLong("id"));
        config.setName(configJson.getString("name"));
        config.setType(configJson.getString("type"));
        config.setCycleType(configJson.getString("cycleType"));
        config.setPriority(configJson.getInteger("priority"));
        config.setRetryMax(configJson.getInteger("retryMax"));
        config.setRetryDur(configJson.getInteger("retryDur"));

        // 调度时间
        config.setSchedulerTime(configJson.getString("schedulerTime"));

        // 表信息
        config.setSourceTable(configJson.getString("sourceTable"));
        config.setTargetTable(configJson.getString("targetTable"));

        // 数据源信息
        if (configJson.containsKey("sourceDb")) {
            config.setSourceDb(parseDataSourceInfo(configJson.getJSONObject("sourceDb")));
        }

        if (configJson.containsKey("targetDb")) {
            config.setTargetDb(parseDataSourceInfo(configJson.getJSONObject("targetDb")));
        }

        // 列配置信息
        if (configJson.containsKey("columnDto")) {
            config.setColumnDto(parseColumnDto(configJson.getJSONObject("columnDto")));
        }

        return config;
    }

    /**
     * 解析数据源信息
     */
    private TaskConfig.DataSourceInfo parseDataSourceInfo(JSONObject dsJson) {
        TaskConfig.DataSourceInfo dsInfo = new TaskConfig.DataSourceInfo();
        dsInfo.setId(dsJson.getLong("id"));
        dsInfo.setDsType(dsJson.getString("dsType"));
        dsInfo.setDsName(dsJson.getString("dsName"));
        dsInfo.setDsUrl(dsJson.getString("dsUrl"));
        dsInfo.setDsUser(dsJson.getString("dsUser"));
        dsInfo.setEncryptPwd(dsJson.getString("encryptPwd"));
        dsInfo.setPwdKey(dsJson.getString("pwdKey"));
        dsInfo.setDbHost(dsJson.getString("dbHost"));
        dsInfo.setDbPort(dsJson.getString("dbPort"));
        dsInfo.setDbName(dsJson.getString("dbName"));
        dsInfo.setStatus(dsJson.getString("status"));
        return dsInfo;
    }

    /**
     * 解析列配置信息
     */
    private TaskConfig.ColumnDto parseColumnDto(JSONObject columnJson) {
        TaskConfig.ColumnDto columnDto = new TaskConfig.ColumnDto();
        columnDto.setIncrementColumn(columnJson.getString("incrementColumn"));
        columnDto.setIncrementType(columnJson.getString("incrementType"));
        columnDto.setIsAllColumn(columnJson.getBoolean("isAllColumn"));
        columnDto.setSplitPk(columnJson.getString("splitPk"));
        columnDto.setWhere(columnJson.getString("where"));

        // 源表列信息
        if (columnJson.containsKey("sourceColumns")) {
            columnDto.setSourceColumns(columnJson.getJSONArray("sourceColumns").toJavaList(String.class));
        }

        return columnDto;
    }

    /**
     * 获取任务配置
     */
    public TaskConfig getTaskConfig() {
        if (taskConfig == null) {
            log.warn("任务配置未加载，请检查配置文件");
        }
        return taskConfig;
    }

    /**
     * 重新加载配置
     */
    public void reloadConfig() {
        log.info("重新加载任务配置");
        loadTaskConfig();
    }

    /**
     * 检查配置是否已加载
     */
    public boolean isConfigLoaded() {
        return taskConfig != null;
    }
}

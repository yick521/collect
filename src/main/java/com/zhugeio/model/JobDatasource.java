package com.zhugeio.model;

import lombok.Data;

@Data
public class JobDatasource {

    private String datasourceType;

    private String jdbcHostName;

    private String jdbcDriverClass;

    private String jdbcUrl;

    private String databaseName;

    private String jdbcUsername;

    private String jdbcPassword;

    private int jdbcPort;

}

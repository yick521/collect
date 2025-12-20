package com.zhugeio;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * DataX同步服务主启动类（命令行应用）
 *
 * @author DataX Team
 * @since 1.0.0
 */
@Slf4j
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableConfigurationProperties
public class DataXSyncApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(DataXSyncApplication.class);
        app.setBannerMode(org.springframework.boot.Banner.Mode.OFF);

        log.info("=================================");
        log.info("DataX同步服务启动中...");
        log.info("=================================");

        // 启动应用
        app.run(args);
    }
}
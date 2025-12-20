package com.zhugeio.model;

import lombok.Data;

/**
 * 增量信息模型
 */
@Data
public class IncrementInfo {

    /**
     * 最大ID值
     */
    private Long maxId;

    /**
     * 最大时间值
     */
    private String maxTime;

    /**
     * 占位符（${maxid} 或 ${maxtime}）
     */
    private String placeholder;

    /**
     * 实际值（用于替换占位符）
     */
    private String realValue;

    /**
     * 是否为时间类型
     */
    private boolean timeType;
}
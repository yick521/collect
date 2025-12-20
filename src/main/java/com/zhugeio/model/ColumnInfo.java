package com.zhugeio.model;

import lombok.Data;

/**
 * 列信息类（需要根据你的实际ColumnInfo类调整）
 */
@Data
public class ColumnInfo {
    private String name;        // 列名
    private String type;        // 原始类型
    private String hiveType;    // Hive类型
    private String webType;     // Web类型
    private Boolean isPrimaryKey; // 是否主键
    private Boolean isSelect;   // 是否选中
    private String comment;
}

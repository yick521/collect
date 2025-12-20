package com.zhugeio.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DasColumn {

    private String columnName;

    private String columnTypeName;

    private String columnClassName;

    private String columnComment;
    // private int isNull;
    private boolean isprimaryKey;
}

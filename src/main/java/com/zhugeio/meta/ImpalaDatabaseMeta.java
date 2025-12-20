package com.zhugeio.meta;

import com.zhugeio.model.ColumnInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @desc
 * @Author Liujh
 * @Date 2022/7/19 9:44
 */
public class ImpalaDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {
    private volatile static ImpalaDatabaseMeta single;

    public static ImpalaDatabaseMeta getInstance() {
        if (single == null) {
            synchronized (ImpalaDatabaseMeta.class) {
                if (single == null) {
                    single = new ImpalaDatabaseMeta();
                }
            }
        }
        return single;
    }

    @Override
    public String getSQLQueryFields(String tableName) {
        return "DESCRIBE " + tableName;
    }

    @Override
    public String getSQLQueryTables() {
        return "show tables";
    }

    @Override
    public String getSQLQueryTables(String tableName) {
        return "show tables like '" + tableName + "'";
    }

    @Override
    public String deleteTable(String tableName) {
        return "drop table if exists " + tableName;
    }

    @Override
    public String createTable(List<ColumnInfo> columnInfos, String tableName) {

        StringBuffer column = new StringBuffer();
        // List<ColumnInfo> collect = columnInfos.stream().filter(s -> s.getIsSelect() == true).collect(Collectors.toList());
        column.append("CREATE TABLE  IF NOT EXISTS ").append(tableName).append(" (");
        for (int i = 0; i < columnInfos.size(); i++) {
            ColumnInfo columnInfo = columnInfos.get(i);
            String name = columnInfo.getName();
            if (name.contains(":")) {
                name = name.substring(name.lastIndexOf(":") + 1);
            }
            column.append("`").append(name).append("` ").append(columnInfos.get(i).getHiveType());
            if (StringUtils.isNotBlank(columnInfos.get(i).getComment())) {
                column.append(" comment '").append(columnInfos.get(i).getComment()).append("'");
            }
            if (i != columnInfos.size() - 1) {
                column.append(",");
            }
        }
        column.append(")").append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' "
        ).append("STORED AS TEXTFILE;");

        return column.toString();
    }
}

package com.zhugeio.meta;


import com.zhugeio.model.ColumnInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @desc Doris数据库 meta信息查询
 * @Author Liujh
 * @Date 2022/7/18 14:47
 */
public class DorisDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

    private volatile static DorisDatabaseMeta single;

    public static DorisDatabaseMeta getInstance() {
        if (single == null) {
            synchronized (ImpalaDatabaseMeta.class) {
                if (single == null) {
                    single = new DorisDatabaseMeta();
                }
            }
        }
        return single;
    }


    @Override
    public String createTable(List<ColumnInfo> columnInfos, String tableName) {
        StringBuffer column = new StringBuffer();
        // List<ColumnInfo> collect = columnInfos.stream().filter(s -> s.getIsSelect() == true).collect(Collectors.toList());
        column.append("CREATE TABLE  IF NOT EXISTS `").append(tableName).append("` (");
        for (int i = 0; i < columnInfos.size(); i++) {
            ColumnInfo columnInfo = columnInfos.get(i);
            String name = columnInfo.getName();
            if (name.contains(":")) {
                name = name.substring(name.lastIndexOf(":") + 1);
            }
            column.append("`").append(name).append("` ").append(columnInfos.get(i).getHiveType().equalsIgnoreCase("date")?"datetime":columnInfos.get(i).getHiveType());
            if (StringUtils.isNotBlank(columnInfos.get(i).getComment())) {
                column.append(" comment '").append(columnInfos.get(i).getComment()).append("'");
            }
            if (i != columnInfos.size() - 1) {
                column.append(",");
            }
        }
        column.append(")")
                .append("DUPLICATE KEY (`").append(columnInfos.get(0).getName()).append("`) \t ")
                .append("DISTRIBUTED BY HASH (`").append(columnInfos.get(0).getName()).append("`) BUCKETS 1 \t ")
                .append("PROPERTIES ( \"replication_allocation\" = \"tag.location.default: 1\" );");
        System.out.println(column.toString());
        return column.toString();
    }


}

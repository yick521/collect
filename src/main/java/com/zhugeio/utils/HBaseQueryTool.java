package com.zhugeio.utils;

import com.zhugeio.model.ColumnBuilder;
import com.zhugeio.model.ColumnInfo;
import com.zhugeio.model.JobDatasource;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName HBaseQueryTool
 * @Desc TODO
 * @Author daijinlin
 * @Date 2023/4/10 14:20
 */
@Slf4j
public class HBaseQueryTool {

    public HBaseQueryTool(JobDatasource jobDatasource)  {
        try {
            getDataSource(jobDatasource);
        } catch (Exception e) {
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }

    private void getDataSource(JobDatasource jobDatasource)  {
        try {
            Configuration conf = HBaseConfiguration.create();
//            System.setProperty("jute.maxbuffer",4096*1024*10000+"");
            conf.set("hbase.zookeeper.property.clientPort", "2182");
            conf.set("hbase.client.write.buffer", "2097152");
//            conf.set("hbase.zookeeper.quorum", jobDatasource.getJdbcHostName()+":"+jobDatasource.getJdbcPort());
            conf.set("hbase.zookeeper.quorum", jobDatasource.getJdbcHostName());
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
//            TableName[] tableNames = admin.listTableNames();
            conn.close();

        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e.getMessage());
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }

    public List<String> getTables(JobDatasource jobDatasource) {
        List<String> tables = new ArrayList<>();
        try {
            Configuration conf = HBaseConfiguration.create();
//            System.setProperty("jute.maxbuffer",4096*1024*10000+"");
            conf.set("hbase.zookeeper.property.clientPort", "2182");
            conf.set("hbase.client.write.buffer", "2097152");
//            conf.set("hbase.zookeeper.quorum", jobDatasource.getJdbcHostName()+":"+jobDatasource.getJdbcPort());
            conf.set("hbase.zookeeper.quorum", jobDatasource.getJdbcHostName());
            conf.set("hbase.rootdir", "hdfs://"+jobDatasource.getJdbcHostName()+":8020/default");
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            TableName[] tableNames = admin.listTableNames();
            for (TableName tableName : tableNames) {
                tables.add(tableName.getNameAsString());
            }
            conn.close();

        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e.getMessage());
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
        return tables;
    }

    public List<ColumnInfo> getColumns(JobDatasource jobDatasource, String tableName) {
        List<ColumnInfo> columns = new ArrayList<>();
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", "2182");
            conf.set("hbase.zookeeper.quorum", jobDatasource.getJdbcHostName());
            Connection conn = ConnectionFactory.createConnection(conf);
            HTable hTable = new HTable(conf, tableName);
            Scan scan = new Scan();
            ResultScanner scanner = hTable.getScanner(scan);
            Result next = scanner.next();
            for (KeyValue keyValue : next.raw()) {
                String family = new String(keyValue.getFamily());
                String qualifier = new String(keyValue.getQualifier());
                ColumnInfo columnInfo = new ColumnInfo();
                columnInfo.setName(family+":"+qualifier);
                columnInfo.setType("string");
                columns.add(columnInfo);
            }
            conn.close();

        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e.getMessage());
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
        return columns;
    }
}

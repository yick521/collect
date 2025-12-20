package com.zhugeio.utils;

import com.zhugeio.model.ColumnInfo;
import com.zhugeio.model.JobDatasource;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName HdfsQueryTool
 * @Desc TODO
 * @Author daijinlin
 * @Date 2023/5/19 15:14
 */
@Slf4j
public class HdfsQueryTool {

    final String DELIMITER = new String(" ");
    final String INNER_DELIMITER = ",";


    public HdfsQueryTool(JobDatasource jobDatasource) {
        try {
            getDataSource(jobDatasource);
        } catch (Exception e) {
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }

    private void getDataSource(JobDatasource jobDatasource)  {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS",jobDatasource.getJdbcUrl());
            configuration.set("hbase.rootdir", "hdfs://"+jobDatasource.getJdbcHostName()+":8020/default");
            FileSystem fileSystem = FileSystem.get(new URI(jobDatasource.getDatabaseName()),configuration,"root");
            boolean exists = fileSystem.exists(new Path(jobDatasource.getDatabaseName()));

            if (exists){
                log.info("connection init success");
                //3.关闭资源
                fileSystem.close();
            }
        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e);
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }

    public List<ColumnInfo> getColumns(JobDatasource jobDatasource, String tableName) {
        List<ColumnInfo> columns = new ArrayList<>();
        String yhdsxCategoryIdStr = "";
        BufferedReader br = null;
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS",jobDatasource.getJdbcUrl());
            configuration.set("hbase.rootdir", "hdfs://"+jobDatasource.getJdbcHostName()+":" + jobDatasource.getJdbcPort() + "/default");
            FileSystem fileSystem = FileSystem.get(new URI(jobDatasource.getDatabaseName()),configuration,"root");
             //3:遍历迭代器
            Path path = new Path(jobDatasource.getDatabaseName() + "/" + tableName);
            FSDataInputStream inputStream = fileSystem.open(path);

            br = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while (null != (line = br.readLine())) {
                String[] strs = line.split(DELIMITER);
                for (int i = 0; i < strs.length; i++) {
                    String str = strs[i];
                    ColumnInfo columnInfo = new ColumnInfo();
                    columnInfo.setName("col"+i);
                    columnInfo.setType("string");
                    columnInfo.setComment(str);
                    columns.add(columnInfo);
                }
                break;
            }// end of while
            fileSystem.close();
        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e);
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
        return columns;
    }

    public List<String> getTables(JobDatasource jobDatasource) {
        List<String> tables = new ArrayList<>();
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS",jobDatasource.getJdbcUrl());
            configuration.set("hbase.rootdir", "hdfs://"+jobDatasource.getJdbcHostName()+":8020/default");
            FileSystem fileSystem = FileSystem.get(new URI(jobDatasource.getDatabaseName()),configuration,"root");
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(jobDatasource.getDatabaseName() + "/"), true);
            //3:遍历迭代器
            while (iterator.hasNext()){
                LocatedFileStatus fileStatus = iterator.next();

                //获取文件的绝对路径 : hdfs://node01:8020/xxx
                //getPath()方法就是获取绝对路径
                String path = fileStatus.getPath().toString();
                String table = path.substring(path.lastIndexOf("/") + 1);
                if (!fileStatus.isDirectory()){
                    tables.add(table);
                }
            }
            fileSystem.close();
        }catch (Exception e) {
            log.error("您所选的数据库连接失败",e);
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
        return tables;
    }

    public void listAllFiles(FileSystem fileSystem,Path path) throws  Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isDirectory()){
                listAllFiles(fileSystem,fileStatus.getPath());
            }else{
                Path path1 = fileStatus.getPath();
                System.out.println("文件路径为"+path1);
            }
        }
    }
}

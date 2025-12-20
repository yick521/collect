package com.zhugeio.utils;


import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.zhugeio.model.ColumnInfo;
import com.zhugeio.model.JobDatasource;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//@Slf4j
public class MongoDBQueryTool  {


    private static MongoClient connection = null;
    private static MongoDatabase collections;
    private static String type = "mongodb";


    public MongoDBQueryTool(JobDatasource jobDatasource)  {
        try {
            getDataSource(jobDatasource);
        } catch (Exception e) {
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }

    private void getDataSource(JobDatasource jobDatasource)  {
        try {
            connection = new MongoClient(new MongoClientURI(jobDatasource.getJdbcUrl()));
            collections = connection.getDatabase(jobDatasource.getDatabaseName());
            MongoDatabase db = connection.getDatabase(jobDatasource.getDatabaseName());
            for (String colName : db.listCollectionNames()){
//                log.info("数据库连接成功");
            }
        }catch (Exception e) {
//            log.error("您所选的数据库连接失败:",e);
            throw new RuntimeException("您所选的数据库连接失败："+e.getMessage());
        }
    }


    // 关闭连接
    public static void sourceClose() {
        if (connection != null) {
            connection.close();
        }
    }

    // public void createCol(){
    //     DB test = connection.getDB("test");
    //     DBCollection company = test.getCollection("company_user");
    //     BasicDBObject doc = new BasicDBObject();
    //     doc.put("name","test");
    //     doc.put("remake","测试");
    //     company.insert(doc);
    //     // connection.close();
    // }

    /**
     * 获取DB名称列表
     *
     * @return
     */
    public List<String> getDBNames() {
        MongoIterable<String> dbs = connection.listDatabaseNames();
        List<String> dbNames = new ArrayList<>();
        dbs.forEach((Block<? super String>) dbNames::add);
        return dbNames;
    }

    /**
     * 测试是否连接成功
     *
     * @return
     */
    public boolean dataSourceTest(String dbName) {
        collections = connection.getDatabase(dbName);
        return collections.listCollectionNames().iterator().hasNext();
    }

    /**
     * 获取Collection名称列表
     *
     * @return
     */
    public List<String> getCollectionNames(String dbName) {
        collections = connection.getDatabase(dbName);
        List<String> collectionNames = new ArrayList<>();
        collections.listCollectionNames().forEach((Block<? super String>) collectionNames::add);
        return collectionNames;
    }

    /**
     * 通过CollectionName查询列
     *
     * @param collectionName
     * @return
     */
    public List<ColumnInfo> getColumns(String collectionName) {
        MongoCollection<Document> collection = collections.getCollection(collectionName);
        Document document = collection.find(new BasicDBObject()).first();
        List<ColumnInfo> list = new ArrayList<>();
        if (null == document || document.size() <= 0) {
            return list;
        }
        document.forEach((k, v) -> {
            if (null != v) {
                ColumnInfo columnInfo = new ColumnInfo();
                String type = v.getClass().getSimpleName();
                if("ObjectId".equalsIgnoreCase(type)){
                    columnInfo.setIsPrimaryKey(true);
                }else {
                    columnInfo.setIsPrimaryKey(false);
                }
                columnInfo.setName(k);
                columnInfo.setType(type);
                list.add(columnInfo);
            }

        });
        return list;
    }

    /**
     * 判断地址类型是否符合要求
     *
     * @param addressList
     * @return
     */
    private static boolean isHostPortPattern(List<Object> addressList) {
        for (Object address : addressList) {
            String regex = "(\\S+):([0-9]+)";
            if (!((String) address).matches(regex)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 转换为mongo地址协议
     *
     * @param rawAddress
     * @return
     */
    private static List<ServerAddress> parseServerAddress(String rawAddress) throws UnknownHostException {
        List<ServerAddress> addressList = new ArrayList<>();
        for (String address : Arrays.asList(rawAddress.split(","))) {
            String[] tempAddress = address.split(":");
            try {
                ServerAddress sa = new ServerAddress(tempAddress[0], Integer.valueOf(tempAddress[1]));
                addressList.add(sa);
            } catch (Exception e) {
                throw new UnknownHostException();
            }
        }
        return addressList;
    }
}

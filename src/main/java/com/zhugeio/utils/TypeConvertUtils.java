package com.zhugeio.utils;

import com.zhugeio.model.ColumnInfo;
import com.zhugeio.model.Constant;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.util.List;

/**
 * @desc
 * @Author Liujh
 * @Date 2022/7/19 14:01
 */
public class TypeConvertUtils {

    public static void typeToWeb(String type, List<ColumnInfo> columnInfos) {
        if (CollectionUtils.isEmpty(columnInfos)) {
            return;
        }
        if (Constant.MYSQL_TYPE.equalsIgnoreCase(type) || Constant.ORACLE_TYPE.equalsIgnoreCase(type)
                || Constant.SQL_SERVER_TYPE.equalsIgnoreCase(type) || Constant.HBASE_DB_TYPE.equalsIgnoreCase(type)
                || Constant.KINGBASE_TYPE.equalsIgnoreCase(type) || Constant.CLICKHOUSE_TYPE.equalsIgnoreCase(type)
                || Constant.STARROCKS_TYPE.equalsIgnoreCase(type) || Constant.OCEAN_BASE_TYPE.equalsIgnoreCase(type)
                || Constant.HDFS_TYPE.equalsIgnoreCase(type) || Constant.API_TYPE.equalsIgnoreCase(type)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.MysqlType.TINYINT) || e.getType().toUpperCase().contains(Constant.MysqlType.SMALLINT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.MEDIUMINT) || e.getType().toUpperCase().contains(Constant.MysqlType.INT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.BIGINT) || e.getType().equalsIgnoreCase(Constant.MysqlType.FLOAT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.DOUBLE) || e.getType().toUpperCase().contains(Constant.MysqlType.DECIMAL)
                        || e.getType().toUpperCase().contains(Constant.MysqlType.NUMBER)
                ) {
                    e.setWebType("number");
                } else if (e.getType().equalsIgnoreCase(Constant.MysqlType.YEAR) || e.getType().equalsIgnoreCase(Constant.MysqlType.TIME) ||
                        e.getType().equalsIgnoreCase(Constant.MysqlType.DATE) || e.getType().equalsIgnoreCase(Constant.MysqlType.DATETIME) ||
                        e.getType().equalsIgnoreCase(Constant.MysqlType.TIMESTAMP)
                ) {
                    e.setWebType("date");
                } else {
                    e.setWebType("string");
                }
            });
        } else if (type.equalsIgnoreCase(Constant.MONGO_DB_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.MongoDBType.DOUBLE) || e.getType().toUpperCase().contains(Constant.MongoDBType.LONG) ||
                        e.getType().toUpperCase().contains(Constant.MongoDBType.INT) || e.getType().toUpperCase().contains(Constant.MongoDBType.DECIMAL)
                ) {
                    e.setWebType("number");
                } else if (e.getType().equalsIgnoreCase(Constant.MongoDBType.DATE)
                ) {
                    e.setWebType("date");
                } else {
                    e.setWebType("string");
                }
            });
        } else if (type.equalsIgnoreCase(Constant.HIVE_TYPE) || type.equalsIgnoreCase(Constant.IMPALA_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.HiveType.TINYINT) || e.getType().toUpperCase().contains(Constant.HiveType.SMALLINT) ||
                        e.getType().toUpperCase().contains(Constant.HiveType.INT) || e.getType().toUpperCase().contains(Constant.HiveType.DECIMAL)
                        || e.getType().toUpperCase().contains(Constant.HiveType.DOUBLE) || e.getType().toUpperCase().contains(Constant.HiveType.FLOAT)
                ) {
                    e.setWebType("number");
                } else if (e.getType().equalsIgnoreCase(Constant.HiveType.DATE) || e.getType().equalsIgnoreCase(Constant.HiveType.TIMESTAMP)
                ) {
                    e.setWebType("date");
                } else {
                    e.setWebType("string");
                }
            });
        } else if (type.equalsIgnoreCase(Constant.POSTGRE_SQL_TYPE) || type.equalsIgnoreCase(Constant.DRDS_TYPE)) {

            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.PostgreSQLType.SMALLINT) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.INTEGER) ||
                        e.getType().toUpperCase().contains(Constant.PostgreSQLType.BIGINT) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.DECIMAL) ||
                        e.getType().toUpperCase().contains(Constant.PostgreSQLType.NUMERIC) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.REAL) ||
                        e.getType().toUpperCase().contains(Constant.PostgreSQLType.DOUBLE) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.SERIAL) ||
                        e.getType().toUpperCase().contains(Constant.PostgreSQLType.BIGSERIAL)
                ) {
                    e.setWebType("number");
                } else if (e.getType().equalsIgnoreCase(Constant.PostgreSQLType.DATE)
                ) {
                    e.setWebType("date");
                } else {
                    e.setWebType("string");
                }
            });
        } else if (type.equalsIgnoreCase(Constant.LOCAL_TYPE) || type.equalsIgnoreCase(Constant.FTP_TYPE) || type.equalsIgnoreCase(Constant.EXCEL_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.ExcelType.NUMERIC)) {
                    e.setWebType("number");
                } else {
                    e.setWebType("string");
                }
            });
        }
    }


    public static void typeToHive(String type, List<ColumnInfo> columnInfos) {
        if (type.equalsIgnoreCase(Constant.MYSQL_TYPE) || type.equalsIgnoreCase(Constant.ORACLE_TYPE)
                || type.equalsIgnoreCase(Constant.SQL_SERVER_TYPE) || type.equalsIgnoreCase(Constant.HBASE_DB_TYPE)
                || type.equalsIgnoreCase(Constant.KINGBASE_TYPE) || type.equalsIgnoreCase(Constant.CLICKHOUSE_TYPE)
                || type.equalsIgnoreCase(Constant.STARROCKS_TYPE) || Constant.OCEAN_BASE_TYPE.equalsIgnoreCase(type) || Constant.API_TYPE.equalsIgnoreCase(type)
                || type.equalsIgnoreCase(Constant.HDFS_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.MysqlType.TINYINT) || e.getType().toUpperCase().contains(Constant.MysqlType.SMALLINT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.MEDIUMINT) || e.getType().toUpperCase().contains(Constant.MysqlType.INT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.BIGINT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.NUMBER)
                ) {
                    e.setHiveType("BIGINT");
                } else if (e.getType().toUpperCase().contains(Constant.MysqlType.FLOAT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.DOUBLE) || e.getType().toUpperCase().contains(Constant.MysqlType.DECIMAL)
                ) {
                    e.setHiveType("DOUBLE");
                } else if (e.getType().equalsIgnoreCase(Constant.MysqlType.YEAR) || e.getType().equalsIgnoreCase(Constant.MysqlType.TIME) ||
                        e.getType().equalsIgnoreCase(Constant.MysqlType.DATE) || e.getType().equalsIgnoreCase(Constant.MysqlType.DATETIME) ||
                        e.getType().equalsIgnoreCase(Constant.MysqlType.TIMESTAMP)
                ) {
                    e.setHiveType("TIMESTAMP");
                } else {
                    e.setHiveType("STRING");
                }
            });
        } else if (type.equalsIgnoreCase(Constant.MONGO_DB_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.MongoDBType.LONG) || e.getType().toUpperCase().contains(Constant.MongoDBType.INT)
                ) {
                    e.setHiveType("BIGINT");
                } else if (e.getType().toUpperCase().contains(Constant.MongoDBType.DOUBLE) || e.getType().toUpperCase().contains(Constant.MongoDBType.DECIMAL)) {
                    e.setHiveType("DOUBLE");
                } else if (e.getType().equalsIgnoreCase(Constant.MongoDBType.DATE)
                ) {
                    e.setHiveType("TIMESTAMP");
                } else {
                    e.setHiveType("STRING");
                }
            });
        } else if (type.equalsIgnoreCase(Constant.HIVE_TYPE) || type.equalsIgnoreCase(Constant.IMPALA_TYPE)) {


            columnInfos.forEach(s ->
                    {
                        if (s.getType().toUpperCase().contains(Constant.HiveType.TIMESTAMP) || s.getType().toUpperCase().contains(Constant.HiveType.DATE)) {
                            s.setHiveType("TIMESTAMP");
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.TINYINT)) {
                            s.setHiveType(Constant.HiveType.TINYINT);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.INT)) {
                            s.setHiveType(Constant.HiveType.INT);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.BIGINT)) {
                            s.setHiveType(Constant.HiveType.BIGINT);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.SMALLINT)) {
                            s.setHiveType(Constant.HiveType.SMALLINT);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.BOOLEAN)) {
                            s.setHiveType(Constant.HiveType.BOOLEAN);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.FLOAT)) {
                            s.setHiveType(Constant.HiveType.FLOAT);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.DOUBLE)) {
                            s.setHiveType(Constant.HiveType.DOUBLE);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.DECIMAL)) {
                            s.setHiveType(Constant.HiveType.DOUBLE);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.VARCHAR) || s.getType().toUpperCase().contains(Constant.HiveType.STRING)) {
                            s.setHiveType(Constant.HiveType.STRING);
                        } else {
                            s.setHiveType(Constant.HiveType.STRING);
                        }
                    }
            );
        } else if (type.equalsIgnoreCase(Constant.POSTGRE_SQL_TYPE) || type.equalsIgnoreCase(Constant.DRDS_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.PostgreSQLType.SMALLINT) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.INTEGER) ||
                        e.getType().toUpperCase().contains(Constant.PostgreSQLType.BIGINT) ||
                        e.getType().toUpperCase().contains(Constant.PostgreSQLType.SERIAL) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.BIGSERIAL)
                ) {
                    e.setHiveType("BIGINT");
                } else if (e.getType().toUpperCase().contains(Constant.PostgreSQLType.DECIMAL) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.NUMERIC) ||
                        e.getType().toUpperCase().contains(Constant.PostgreSQLType.DOUBLE) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.REAL)) {
                    e.setHiveType("DOUBLE");
                } else if (e.getType().equalsIgnoreCase(Constant.PostgreSQLType.DATE) || e.getType().equalsIgnoreCase(Constant.PostgreSQLType.TIME) || e.getType().equalsIgnoreCase(Constant.PostgreSQLType.TIMESTAMP)
                ) {
                    e.setHiveType("TIMESTAMP");
                } else {
                    e.setHiveType("STRING");
                }
            });
        } else if (type.equalsIgnoreCase(Constant.LOCAL_TYPE) || type.equalsIgnoreCase(Constant.FTP_TYPE) || type.equalsIgnoreCase(Constant.EXCEL_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.ExcelType.NUMERIC)) {
                    e.setHiveType("DOUBLE");
                } else {
                    e.setHiveType("STRING");
                }
            });
        }
    }


    public static void typeToDoris(String type, List<ColumnInfo> columnInfos) {
        if (type.equalsIgnoreCase(Constant.MYSQL_TYPE) || type.equalsIgnoreCase(Constant.ORACLE_TYPE)
                || type.equalsIgnoreCase(Constant.SQL_SERVER_TYPE) || type.equalsIgnoreCase(Constant.HBASE_DB_TYPE)
                || type.equalsIgnoreCase(Constant.KINGBASE_TYPE) || type.equalsIgnoreCase(Constant.CLICKHOUSE_TYPE)
                || type.equalsIgnoreCase(Constant.STARROCKS_TYPE) || Constant.OCEAN_BASE_TYPE.equalsIgnoreCase(type) || Constant.API_TYPE.equalsIgnoreCase(type)
                || type.equalsIgnoreCase(Constant.HDFS_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.MysqlType.TINYINT) || e.getType().toUpperCase().contains(Constant.MysqlType.SMALLINT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.MEDIUMINT) || e.getType().toUpperCase().contains(Constant.MysqlType.INT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.BIGINT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.NUMBER)
                ) {
                    e.setHiveType("BIGINT");
                } else if (e.getType().toUpperCase().contains(Constant.MysqlType.FLOAT) ||
                        e.getType().toUpperCase().contains(Constant.MysqlType.DOUBLE) || e.getType().toUpperCase().contains(Constant.MysqlType.DECIMAL)
                ) {
                    e.setHiveType("DOUBLE");
                } else if (e.getType().equalsIgnoreCase(Constant.MysqlType.YEAR) || e.getType().equalsIgnoreCase(Constant.MysqlType.TIME) ||
                        e.getType().equalsIgnoreCase(Constant.MysqlType.DATE) || e.getType().equalsIgnoreCase(Constant.MysqlType.DATETIME) ||
                        e.getType().equalsIgnoreCase(Constant.MysqlType.TIMESTAMP)
                ) {
                    e.setHiveType("DATE");
                } else {
                    e.setHiveType("VARCHAR(255)");
                }
            });
        } else if (type.equalsIgnoreCase(Constant.MONGO_DB_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.MongoDBType.LONG) || e.getType().toUpperCase().contains(Constant.MongoDBType.INT)
                ) {
                    e.setHiveType("BIGINT");
                } else if (e.getType().toUpperCase().contains(Constant.MongoDBType.DOUBLE) || e.getType().toUpperCase().contains(Constant.MongoDBType.DECIMAL)) {
                    e.setHiveType("DOUBLE");
                } else if (e.getType().equalsIgnoreCase(Constant.MongoDBType.DATE)
                ) {
                    e.setHiveType("DATE");
                } else {
                    e.setHiveType("VARCHAR(255)");
                }
            });
        } else if (type.equalsIgnoreCase(Constant.HIVE_TYPE) || type.equalsIgnoreCase(Constant.IMPALA_TYPE)) {
            columnInfos.forEach(s ->
                    {
                        if (s.getType().toUpperCase().contains(Constant.HiveType.TIMESTAMP) || s.getType().toUpperCase().contains(Constant.HiveType.DATE)) {
                            s.setHiveType("TIMESTAMP");
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.TINYINT)) {
                            s.setHiveType(Constant.HiveType.TINYINT);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.INT)) {
                            s.setHiveType(Constant.HiveType.INT);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.BIGINT)) {
                            s.setHiveType(Constant.HiveType.BIGINT);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.SMALLINT)) {
                            s.setHiveType(Constant.HiveType.SMALLINT);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.BOOLEAN)) {
                            s.setHiveType(Constant.HiveType.BOOLEAN);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.FLOAT)) {
                            s.setHiveType(Constant.HiveType.FLOAT);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.DOUBLE)) {
                            s.setHiveType(Constant.HiveType.DOUBLE);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.DECIMAL)) {
                            s.setHiveType(Constant.HiveType.DOUBLE);
                        } else if (s.getType().toUpperCase().contains(Constant.HiveType.VARCHAR) || s.getType().toUpperCase().contains(Constant.HiveType.STRING)) {
                            s.setHiveType(Constant.HiveType.STRING);
                        } else {
                            s.setHiveType(Constant.HiveType.STRING);
                        }
                    }
            );
        } else if (type.equalsIgnoreCase(Constant.POSTGRE_SQL_TYPE) || type.equalsIgnoreCase(Constant.DRDS_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.PostgreSQLType.SMALLINT) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.INTEGER) ||
                        e.getType().toUpperCase().contains(Constant.PostgreSQLType.BIGINT) ||
                        e.getType().toUpperCase().contains(Constant.PostgreSQLType.SERIAL) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.BIGSERIAL)
                ) {
                    e.setHiveType("BIGINT");
                } else if (e.getType().toUpperCase().contains(Constant.PostgreSQLType.DECIMAL) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.NUMERIC) ||
                        e.getType().toUpperCase().contains(Constant.PostgreSQLType.DOUBLE) || e.getType().toUpperCase().contains(Constant.PostgreSQLType.REAL)) {
                    e.setHiveType("DOUBLE");
                } else if (e.getType().equalsIgnoreCase(Constant.PostgreSQLType.DATE) || e.getType().equalsIgnoreCase(Constant.PostgreSQLType.TIME) || e.getType().equalsIgnoreCase(Constant.PostgreSQLType.TIMESTAMP)
                ) {
                    e.setHiveType("DATE");
                } else {
                    e.setHiveType("VARCHAR(255)");
                }
            });
        } else if (type.equalsIgnoreCase(Constant.LOCAL_TYPE) || type.equalsIgnoreCase(Constant.FTP_TYPE) || type.equalsIgnoreCase(Constant.EXCEL_TYPE)) {
            columnInfos.forEach(e -> {
                if (e.getType().toUpperCase().contains(Constant.ExcelType.NUMERIC)) {
                    e.setHiveType("DATE");
                } else {
                    e.setHiveType("VARCHAR(255)");
                }
            });
        }
    }


    public static String defaultTypeByWebType(String webType) {
        if (Constant.WebType.NUMBER.equals(webType)) {
            return Constant.HiveType.DOUBLE;
        }

        if (Constant.WebType.DATE.equals(webType)) {
            return Constant.HiveType.TIMESTAMP;
        }

        if (Constant.WebType.STRING.equals(webType)) {
            return Constant.HiveType.STRING;
        }
        return null;
    }

    /***
     * 功能描述: list<bean> 深拷贝
     * (bean 对象必须序列化，即 implements Serializable)
     */
    public static <T> List<T> deepCopyListBean(List<T> src) {
        try {
            //目标输出流
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            //对象输出流
            ObjectOutputStream out = null;
            out = new ObjectOutputStream(byteOut);
            //对参数指定的obj对象进行序列化，把得到的字节序列写到一个目标输出流中
            out.writeObject(src);
            ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
            ObjectInputStream in = new ObjectInputStream(byteIn);//对象输入流
            //readObject()方法从一个源输入流中读取 字节序列，再把它们反序列化为一个对象
            return (List<T>) in.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return null;
    }


}

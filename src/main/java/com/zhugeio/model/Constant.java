package com.zhugeio.model;

public class Constant {

    public static final String MYSQL_TYPE = "mysql";
    public static final String MYSQL_DRIVE_CLASS = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL_PREFIX = "jdbc:mysql://";
    public static final String MYSQL_URL_SUFFIX = "?useUnicode=true&characterEncoding=UTF-8&useTimezone=true&serverTimezone=GMT%2B8";


    public static final String ORACLE_TYPE = "oracle";
    public static final String ORACLE_DRIVE_CLASS = "oracle.jdbc.driver.OracleDriver";
    public static final String ORACLE_URL_PREFIX = "jdbc:oracle:thin:@";

    public static final String SQL_SERVER_TYPE = "sqlserver";
    public static final String SQL_SERVER_DRIVE_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String SQL_SERVER_URL_PREFIX = "jdbc:sqlserver://";

    public static final String KINGBASE_TYPE = "kingbase";
    public static final String KINGBASE_DRIVE_CLASS = "com.kingbase8.Driver";
    public static final String KINGBASE_URL_PREFIX = "jdbc:kingbase8://";
    public static final String KINGBASE_URL_SUFFIX = "?useUnicode=true&characterEncoding=UTF-8&useTimezone=true&serverTimezone=GMT%2B8";

    public static final String CLICKHOUSE_TYPE = "clickhouse";
    public static final String CLICKHOUSE_DRIVE_CLASS = "ru.yandex.clickhouse.ClickHouseDriver";  //官方包
    //	public static final String CLICKHOUSE_DRIVE_CLASS = "com.github.housepower.jdbc.ClickHouseDriver";	//第三方包
    public static final String CLICKHOUSE_URL_PREFIX = "jdbc:clickhouse://";
    public static final String CLICKHOUSE_URL_SUFFIX = "?useUnicode=true&characterEncoding=UTF-8&useTimezone=true&serverTimezone=GMT%2B8";

    public static final String OCEAN_BASE_TYPE = "oceanbase";
    public static final String OCEAN_BASE_DRIVE_CLASS = "com.oceanbase.jdbc.Driver";
    public static final String OCEAN_BASE_URL_PREFIX = "jdbc:oceanbase://";
    public static final String OCEAN_BASE_URL_SUFFIX = "?useUnicode=true&characterEncoding=UTF-8&useTimezone=true&serverTimezone=GMT%2B8";

    public static final String DORIS_TYPE = "doris";
    public static final String DORIS_DRIVE_CLASS = "com.mysql.cj.jdbc.Driver";
    public static final String DORIS_URL_PREFIX = "jdbc:mysql://";

    public static final String POSTGRE_SQL_TYPE = "postgresql";
    public static final String POSTGRE_SQL_DRIVE_CLASS = "org.postgresql.Driver";
    public static final String POSTGRE_SQL_URL_PREFIX = "jdbc:postgresql://";

    public static final String HDFS_TYPE = "hdfs";
    public static final String HDFS_URL_PREFIX = "hdfs://";

    public static final String STARROCKS_TYPE = "starrocks";

    public static final String HBASE_DB_TYPE = "hbase";

    public static final String IMPALA_TYPE = "impala";
    public static final String IMPALA_DRIVE_CLASS = "com.cloudera.impala.jdbc.Driver";
    public static final String IMPALA_URL_PREFIX = "jdbc:impala://";

    public static final String HIVE_TYPE = "hive";

    public static final String API_TYPE = "api";

    public static final String FTP_TYPE = "ftp";
    public static final String LOCAL_TYPE = "local";
    public static final String EXCEL_TYPE = "excel";

    /**
     * mongodb驱动：org.apache.hive.jdbc.HiveDriver
     * mongodb连接url：mongodb://userName:password@host/?authSource=databaseName&ssh=true;
     */
    public static final String MONGO_DB_TYPE = "mongodb";
    public static final String MONGO_DB_URL_PREFIX = "mongodb://";

    public static final String DRDS_TYPE = "drds";

    /**
     * impala zanalytics库
     */
    public static final String IMPALA_ANALYTICS = "impala_analytics";


    public class MysqlType {
        //mysql整数类型
        public static final String TINYINT = "TINYINT";
        public static final String NUMBER = "NUMBER";
        public static final String SMALLINT = "SMALLINT";
        public static final String MEDIUMINT = "MEDIUMINT";
        public static final String INT = "INT";
        public static final String BIGINT = "BIGINT";
        //mysql小数类型
        public static final String FLOAT = "FLOAT";
        public static final String DOUBLE = "DOUBLE";
        public static final String DECIMAL = "DECIMAL";
        //mysql日期格式
        public static final String YEAR = "YEAR";
        public static final String TIME = "TIME";
        public static final String DATE = "DATE";
        public static final String DATETIME = "DATETIME";
        public static final String TIMESTAMP = "TIMESTAMP";
        //mysql字符串
        public static final String CHAR = "CHAR";
        public static final String VARCHAR = "VARCHAR";
        public static final String TINYTEXT = "TINYTEXT";
        public static final String MEDIUMTEXT = "MEDIUMTEXT";
        public static final String LONGTEXT = "LONGTEXT";
        public static final String TEXT = "TEXT";
        public static final String ENUM = "ENUM";
        public static final String SET = "SET";
        //mysql二进制
        public static final String BIT = "BIT";
        public static final String BINARY = "BINARY";
        public static final String VARBINARY = "VARBINARY";
        public static final String TINYBLOB = "TINYBLOB";
        public static final String BLOB = "BLOB";
        public static final String MEDIUMBLOB = "MEDIUMBLOB";
        public static final String LONGBLOB = "LONGBLOB";
    }

    public class MongoDBType {
        //mongodb数值类型

        public static final String DOUBLE = "DOUBLE";
        public static final String LONG = "LONG";
        public static final String INT = "INT";
        public static final String DECIMAL = "DECIMAL";

        //mongodb字符型
        public static final String OBJECT = "OBJECT";
        public static final String ARRAY = "ARRAY";
        public static final String BINARY_DATA = "Binary data";
        public static final String UNDEFINED = "Undefined";
        public static final String OBJECT_ID = "ObjectId";
        public static final String BOOLEAN = "Boolean";
        public static final String STRING = "STRING";
        public static final String NULL = "Null";
        public static final String REGULAR_EXPRESSION = "Regular Expression";
        public static final String DB_POINTER = "DBPointer";
        public static final String JAVA_SCRIPT = "JavaScript";
        public static final String SYMBOL = "Symbol";
        public static final String TIMESTAMP = "Timestamp";

        //mongodb日期格式
        public static final String DATE = "DATE";
    }

    public class HiveType {
        //数值型
        public static final String TINYINT = "TINYINT";
        public static final String SMALLINT = "SMALLINT";
        public static final String INT = "INT";
        public static final String BIGINT = "BIGINT";
        public static final String BOOLEAN = "BOOLEAN";


        public static final String FLOAT = "FLOAT";
        public static final String DOUBLE = "DOUBLE";
        public static final String DECIMAL = "DECIMAL";
        //字符型
        public static final String STRING = "STRING";
        public static final String VARCHAR = "VARCHAR";


        //日期型
        public static final String TIMESTAMP = "TIMESTAMP";
        public static final String DATE = "DATE";
    }

    public class ExcelType {
        //数值型
        public static final String TINYINT = "TINYINT";
        public static final String NUMERIC = "NUMERIC";
        public static final String TIME = "TIME";
        public static final String REAL = "REAL";
        public static final String SMALLINT = "SMALLINT";
        public static final String INT = "INT";
        public static final String BIGINT = "BIGINT";
        public static final String BOOLEAN = "BOOLEAN";


        public static final String FLOAT = "FLOAT";
        public static final String DOUBLE = "DOUBLE";
        public static final String DECIMAL = "DECIMAL";
        //字符型
        public static final String STRING = "STRING";
        public static final String VARCHAR = "VARCHAR";


        //日期型
        public static final String TIMESTAMP = "TIMESTAMP";
        public static final String DATE = "DATE";
    }

    public class PostgreSQLType {
        //数值型
        public static final String SMALLINT = "SMALLINT";
        public static final String INTEGER = "INT";
        public static final String BIGINT = "BIGINT";
        public static final String DECIMAL = "DECIMAL";
        public static final String NUMERIC = "NUMERIC";
        public static final String REAL = "REAL";
        public static final String DOUBLE = "DOUBLE";
        public static final String SERIAL = "SERIAL";
        public static final String BIGSERIAL = "BIGSERIAL";
        //字符型
        public static final String CHAR = "CHAR";
        public static final String CHARACTER = "CHARACTER";
        public static final String VARCHAR = "VARCHAR";
        public static final String CHARACTER_VARYING = "character varying";
        public static final String TEXT = "TEXT";

        //日期型
        public static final String TIMESTAMP = "TIMESTAMP";
        public static final String TIME = "TIME";
        public static final String DATE = "DATE";
    }

    public class WebType {
        public static final String NUMBER = "number";
        public static final String STRING = "string";
        public static final String DATE = "date";

    }



}

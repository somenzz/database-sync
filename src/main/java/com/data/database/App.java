package com.data.database;

import java.io.File;
import java.io.Reader;
import java.sql.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.data.database.api.impl.DataBaseSync;
import com.data.database.api.impl.PostgresDataBaseSync;

import java.util.ArrayList;
import java.util.List;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.data.database.api.MyEnum.ColSizeTimes;

// import org.apache.logging.log4j.Logger;
// import org.apache.logging.log4j.LogManager;

import org.apache.log4j.Logger;

/**
 * Hello world!
 */
public final class App {

    private static final Logger logger = Logger.getLogger(App.class);

    private App() {
    }

    /**
     * Says hello to the world.
     *
     * @param args The arguments of the program.
     * @throws ClassNotFoundException
     * @throws SQLException
     */

    /**
     * * 读取json文件，返回json串
     *
     * @param fileName
     * @return
     */
    public static String readJsonFile(String fileName) {
        String jsonStr = "";
        try {
            File jsonFile = new File(fileName);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), "utf-8");
            int ch = 0;
            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String compareColMetaData(String toType, String toSchema, String toTable, ResultSet fromColumnMeta,
            ResultSet toColumnMeta, ColSizeTimes colSizeTimes) throws SQLException {
        StringBuilder sb = new StringBuilder();
        while (fromColumnMeta.next() && toColumnMeta.next()) {
            Integer colType = fromColumnMeta.getInt("DATA_TYPE");

            if (colType == Types.CHAR || colType == Types.VARCHAR) {// 处理字符串类型
                if (fromColumnMeta.getInt("COLUMN_SIZE") * colSizeTimes.getTimes() != toColumnMeta
                        .getInt("COLUMN_SIZE")) {
                    if ("db2".equals(toType) || "edw".equals(toType)) {
                        sb.append(String.format("alter table %s.%s alter column %s set data type varchar(%d);",
                                toSchema, toTable, fromColumnMeta.getString("COLUMN_NAME"),
                                colSizeTimes.getTimes() * fromColumnMeta.getInt("COLUMN_SIZE")));
                    } else if ("mysql".equals(toType) || "oracle".equals(toType)) {
                        sb.append(String.format("alter table %s.%s modify column %s varchar(%d);", toSchema, toTable,
                                fromColumnMeta.getString("COLUMN_NAME"),
                                colSizeTimes.getTimes() * fromColumnMeta.getInt("COLUMN_SIZE")));
                    } else if ("postgres".equals(toType) || "elk".equals(toType)) {
                        sb.append(String.format("alter table %s.%s alter column %s type character varying(%d);",
                                toSchema, toTable, fromColumnMeta.getString("COLUMN_NAME"),
                                colSizeTimes.getTimes() * fromColumnMeta.getInt("COLUMN_SIZE")));
                    }
                }

            }

            if (colType == Types.DECIMAL) {// 处理小数字段类型
                if (fromColumnMeta.getInt("COLUMN_SIZE") != toColumnMeta.getInt("COLUMN_SIZE")
                        || fromColumnMeta.getInt("DECIMAL_DIGITS") != toColumnMeta.getInt("DECIMAL_DIGITS")) {
                    if ("db2".equals(toType) || "edw".equals(toType)) {
                        sb.append(String.format("alter table %s.%s alter column %s set data type decimal(%d,%d);",
                                toSchema, toTable, fromColumnMeta.getString("COLUMN_NAME"),
                                fromColumnMeta.getInt("COLUMN_SIZE"), fromColumnMeta.getInt("DECIMAL_DIGITS")));
                    } else if ("mysql".equals(toType) || "oracle".equals(toType)) {
                        sb.append(String.format("alter table %s.%s modify column %s decimal(%d,%d);", toSchema, toTable,
                                fromColumnMeta.getString("COLUMN_NAME"), fromColumnMeta.getInt("COLUMN_SIZE"),
                                fromColumnMeta.getInt("DECIMAL_DIGITS")));
                    } else if ("postgres".equals(toType) || "elk".equals(toType)) {
                        sb.append(String.format("alter table %s.%s alter column %s type decimal(%d,%d);", toSchema,
                                toTable, fromColumnMeta.getString("COLUMN_NAME"), fromColumnMeta.getInt("COLUMN_SIZE"),
                                fromColumnMeta.getInt("DECIMAL_DIGITS")));
                    }
                }
            }
        }
        try {
            // 说明源系统加了字段
            fromColumnMeta.getObject(1);
            logger.warn(String.format(
                    "the source table of %s.%s has been added some columns, do analysis to decide to add.", toSchema,
                    toTable));
        } catch (SQLException e) {
        } finally {
        }
        return sb.toString();
    }

    public static void printHelp() {
        System.out.println(
                "Usage: \njava -jar database-sync-1.0.jar [options] {fromDB} {fromSchema} {fromTable} {toDB} {toSchema} {toTable} [whereClause]");
        System.out.println("options:");
        System.out.println("        --version or -v  :print version then exit");
        System.out.println("        --help or -h     :print help info then exit");
        System.out.println(
                "        --simple or -s   :use insert into table A select * from B mode, ignore table's structure");
        System.out.println("        --from_fields={col1,col2} or -ff={col3,col4}   :specify from fields");
        System.out.println("        --to_fields={col1,col2} or -tf={col3,col4}   :specify to fields");

    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException {

        boolean isSimple = false; /* 是否直接抽取数据，无需判断表结构信息差异 */
        ArrayList<String> argList = new ArrayList<>(); /* 真正用到的参数 */
        ArrayList<String> toTableFields = new ArrayList<>(); /* 指定插入目标表哪些字段 */
        ArrayList<String> fromTableFields = new ArrayList<>(); /* 指定插入目标表哪些字段 */

        for (String arg : args) {
            if ("--version".equals(arg) || "-v".equals(arg)) {
                System.out.println("database-sync v1.1");
                return;
            } else if ("--help".equals(arg) || "-h".equals(arg)) {
                printHelp();
                return;
            } else if ("--simple".equals(arg) || "-s".equals(arg)) {
                logger.info("use insert into table A select * from B mode.");
                isSimple = true;
            } else if (arg.startsWith("--to_fields=") || arg.startsWith("-tf=")) {
                String fields = arg.replace("--to_fields=", "").replace("-tf=", "");
                for (String s : fields.split(",")) {
                    toTableFields.add(s);
                }
            } else if (arg.startsWith("--from_fields=") || arg.startsWith("-ff=")) {
                String fields = arg.replace("--from_fields=", "").replace("-ff=", "");
                for (String s : fields.split(",")) {
                    fromTableFields.add(s);
                }
            } else {
                argList.add(arg);
            }
        }
        if (argList.size() < 6) {
            printHelp();
            return;
        }

        String currentPath = System.getProperty("user.dir");
        logger.info("current path: " + currentPath);

        String fromDb = argList.get(0);
        String fromSchema = argList.get(1);
        String fromTable = argList.get(2);
        String toDb = argList.get(3);
        String toSchema = argList.get(4);
        String toTable = argList.get(5);
        String whereClause = argList.size() == 7 ? argList.get(6) : "";

        if (fromDb.equalsIgnoreCase(toDb) && fromSchema.equalsIgnoreCase(toSchema)
                && fromTable.equalsIgnoreCase(toTable)) {
            System.out.println("You don't need to do that.");
            return;
        }

        whereClause = whereClause.replaceAll("(?i)where", "");

        logger.info(String.format(
                "Your input params are:\nfromDb = %-10s\tfromSchema = %-10s\tfromTable = %-10s\ntoDb = %-10s\ttoSchema = %-10s\ttoTable = %-10s\nwhereClause=%-10s",
                fromDb, fromSchema, fromTable, toDb, toSchema, toTable, whereClause));
        // String fromDb = "wbsj";
        // String fromSchema = "wbsj";
        // String fromTable = "zz_test";
        // String toDb = "postgres";
        // String toSchema = "wbsj";
        // String toTable = "zz_test";
        // String whereClause = null;

        JSONObject jobj = JSON.parseObject(readJsonFile("./config/config.json"));
        String fromJDBC_DRIVER = jobj.getJSONObject(fromDb).getString("driver");
        String fromDB_URL = jobj.getJSONObject(fromDb).getString("url");
        String fromUSER = jobj.getJSONObject(fromDb).getString("user");
        String fromPASS = jobj.getJSONObject(fromDb).getString("password");
        String fromType = jobj.getJSONObject(fromDb).getString("type");
        String fromCoding = jobj.getJSONObject(fromDb).getString("encoding");

        String toJDBC_DRIVER = jobj.getJSONObject(toDb).getString("driver");
        String toDB_URL = jobj.getJSONObject(toDb).getString("url");
        String toUSER = jobj.getJSONObject(toDb).getString("user");
        String toPASS = jobj.getJSONObject(toDb).getString("password");
        String toType = jobj.getJSONObject(toDb).getString("type");
        String toCoding = jobj.getJSONObject(toDb).getString("encoding");

        Integer bufferRows = (Integer) jobj.getOrDefault("buffer-rows", 100000);

        logger.info(
                String.format("begin %s.%s.%s -> %s.%s.%s", fromDb, fromSchema, fromTable, toDb, toSchema, toTable));
        try {
            DataBaseSync fromDataBase = new DataBaseSync(fromType, fromJDBC_DRIVER, fromDB_URL, fromUSER, fromPASS,
                    bufferRows);
            DataBaseSync toDataBase = null;
            if ("postgres".equals(toType) || "elk".equals(toType)) {
                toDataBase = new PostgresDataBaseSync(toType, toJDBC_DRIVER, toDB_URL, toUSER, toPASS, bufferRows);
            } else {
                toDataBase = new DataBaseSync(toType, toJDBC_DRIVER, toDB_URL, toUSER, toPASS, bufferRows);
            }
            // List<String> cols = ds.getTableColumns("SYSCAT", "TABLES");
            // String ddl = ds.getDDL("mysql","apidb","user",ColSizeTimes.DOUBLE);
            // for (String col : cols) {
            // System.out.println(col);
            // }
            List<String> fromColumnNames = new ArrayList<>();
            List<String> toColumnNames = new ArrayList<>();

            if (toTableFields.size() > 0) {
                //如果指定目标列，则使用目标列，不指定则自动获取
                toColumnNames = toTableFields;
            } else {
                toColumnNames = toDataBase.getTableColumns(toSchema, toTable);
            }

            if (fromTableFields.size() > 0) {
                //如果指定源列，则使用源列，不指定则 使用目标列
                fromColumnNames = fromTableFields;
            } else {
                fromColumnNames = toColumnNames;
            }

            if (isSimple) {
                // 处理简单模式
                ResultSet rs = fromDataBase.readData(fromSchema, fromTable, fromColumnNames, whereClause);
                toDataBase.writeData(toSchema, toTable, toColumnNames, rs, whereClause);
                logger.info(String.format("finished (%s)%s.%s -> (%s)%s.%s", fromDb, fromSchema, fromTable, toDb,
                        toSchema, toTable));
                return;
            }

            // 如果来自 edw 由于是 gbk 编码，因此自动长度扩大两倍。
            ColSizeTimes colTimes = "gbk".equals(fromCoding) && "utf-8".equals(toCoding) ? ColSizeTimes.DOUBLE
                    : ColSizeTimes.EQUAL;
            if (colTimes != ColSizeTimes.EQUAL) {
                logger.info(
                        String.format("The varchar column length of target table is %d times that of the source table",
                                colTimes.getTimes()));
            }

            if (!toDataBase.existsTable(toSchema, toTable)) {
                String ddl = fromDataBase.getDDL(toType, fromSchema, fromTable, colTimes);
                try {
                    toDataBase.createTable(toSchema, toTable, ddl);
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            } else {

                ResultSet fromColMeta = fromDataBase.getColMetaData(fromSchema, fromTable);
                ResultSet toColMeta = toDataBase.getColMetaData(toSchema, toTable);
                /* 同步扩充字段长度 */
                String alterSql = compareColMetaData(toType, toSchema, toTable, fromColMeta, toColMeta, colTimes);
                for (String sql : alterSql.split(";")) {
                    if (!sql.isEmpty()) {
                        logger.info(sql);
                        try {
                            toDataBase.executeUpdateSQL(sql);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            ResultSet rs = fromDataBase.readData(fromSchema, fromTable, fromColumnNames, whereClause);
            toDataBase.writeData(toSchema, toTable, toColumnNames, rs, whereClause);
            logger.info(String.format("finished (%s)%s.%s -> (%s)%s.%s", fromDb, fromSchema, fromTable, toDb, toSchema,
                    toTable));
            // DataBaseSync clear = (DataBaseSync) toDataBase;
            // clear.dropTable(toSchema,toTable);
        } finally {

        }
    }
}

package com.data.database;

import java.sql.*;

import com.data.database.utils.Tools;
import java.io.IOException;

import com.data.database.api.impl.DataBaseSyncFactory;
import com.data.database.api.impl.DataBaseSync;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.List;
import com.data.database.api.MyEnum.ColSizeTimes;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * 数据库表同步主程序
 */
public final class App {

    private static final Logger logger = LogManager.getLogger(App.class);

    private String fromDBId;   //源数据库标识符
    private String fromSchema; //源数据库中的模式名
    private String fromTable;  //源数据库表名
    private String toDBId;     //目标数据库标识符
    private String toSchema;   //目标数据库的模式名
    private String toTable;    //目标数据库的表名

    //可选参数
    private boolean isSyncTableDDL = false;// 默认不同步表结构
    private boolean isSpecificFeture = true; //某些具体数据库会有自己特定的支持，比如 PostGressSql 中的 copy 模式会比 jdbc 一行一行插入要快的多。为 true 表示优先使用具体特性，否则仅使用 jdbc 同步。
    private String whereClause; //where 子句的条件，适用于增量同步，应当检查安全性，防止sql注入
    private List<String> toTableFields; /* 指定插入目标表哪些字段 */
    private List<String> fromTableFields; /* 指定插入目标表哪些字段 */

    public String getFromDBId(){
        return this.fromDBId;
    }
    public String getFromSchema(){
        return this.fromSchema;
    }
    public String getFromTable(){
        return this.fromTable;
    }
    public String getToDbId(){
        return this.toDBId;
    }
    public String getToSchema(){
        return this.toSchema;
    }
    public String getToTable(){
        return this.toTable;
    }
    

    public boolean getIsSyncTableDDL(){
        return this.isSyncTableDDL;
    }

    public boolean getIsSpecificFeture(){
        return this.isSpecificFeture;
    }

    public List<String> getFromTableFields(){
        return this.fromTableFields;
    }
    public List<String> getToTableFields(){
        return this.toTableFields;
    }

    public String getWhereClause(){
        return this.whereClause;
    }

    /*处理并检查输入的参数，如果返回 false，则在主函数中退出程序*/
    public boolean handleCommandLineArgs(String[] args){
        List<String> argList = new ArrayList<>();
        for (String arg : args) {
            if ("--version".equals(arg) || "-v".equals(arg)) {
                System.out.println("database-sync v1.3");
                return false;
            } else if ("--help".equals(arg) || "-h".equals(arg)) {
                printHelp();
                return false;
            } else if ("--sync-ddl".equals(arg) || "-sd".equals(arg)) {
                //同步表结构一般情况下比较危险，默认不同步表结构
                this.isSyncTableDDL = true;
            } else if ("--no-feature".equals(arg) || "-nf".equals(arg)) {
                this.isSpecificFeture = false;

            } else if (arg.startsWith("--to-fields=") || arg.startsWith("-tf=")) {
                String fields = arg.replace("--to-fields=", "").replace("-tf=", "");
                for (String s : fields.split(",")) {
                    this.toTableFields.add(s);
                }
            } else if (arg.startsWith("--from-fields=") || arg.startsWith("-ff=")) {
                String fields = arg.replace("--from-fields=", "").replace("-ff=", "");
                for (String s : fields.split(",")) {
                    this.fromTableFields.add(s);
                }
            } else {
                argList.add(arg);
            }
        }
        if (argList.size() < 6) {
            printHelp();
            return false;
        }


        this.fromDBId = argList.get(0);
        this.fromSchema = argList.get(1);
        this.fromTable = argList.get(2);
        this.toDBId = argList.get(3);
        this.toSchema = argList.get(4);
        this.toTable = argList.get(5);
        this.whereClause = argList.size() >= 7 ? argList.get(6) : "";

        if (this.fromDBId.equalsIgnoreCase(this.toDBId) && this.fromSchema.equalsIgnoreCase(this.toSchema)
                && this.fromTable.equalsIgnoreCase(this.toTable)) {
            System.out.println("You don't need to do that.");
            return false;
        }

        this.whereClause = this.whereClause.replaceAll("(?i)where", "");

        String paramsPrint = String.format(
                "Your input params are:\n"
                        + "<=== fromDb=%s, fromSchema=%s, fromTable=%s, fromTableFields=%s, whereClause=%s\n"
                        + "===> toDb=%s, toSchema=%s, toTable=%s, toTableFields=%s",
                this.fromDBId, this.fromSchema, this.fromTable,
                this.fromTableFields.size() > 0 ? String.join(",", this.fromTableFields) : "[toTableFields]", this.whereClause, this.toDBId,
                this.toSchema, this.toTable, this.toTableFields.size() > 0 ? String.join(",", this.toTableFields) : "*");

        logger.info(paramsPrint);
        logger.info("argList=" + String.join(",", argList));

        return true;
    }


    private App() {
        //构造函数，初始化使用
        this.toTableFields = new ArrayList<>();
        this.fromTableFields = new ArrayList<>();
    }

    public static App getInstance(){
        //for test
        return new App();
    }

    /**
     * 比较原表与目标表的表结构，获取表结构同步语句：添加、删除字段，扩长度等等。
     *
     * @param args The arguments of the program.
     * @throws ClassNotFoundException
     * @throws SQLException
     */

    public static String getAlterSqlFromMetaData(String toType, String toSchema, String toTable, ResultSet fromColumnMeta,
            ResultSet toColumnMeta, ColSizeTimes colSizeTimes) throws SQLException {
        StringBuilder sb = new StringBuilder();

        HashMap<String, HashMap<String, Object>> fromColMap = new HashMap<String, HashMap<String, Object>>();
        HashMap<String, HashMap<String, Object>> toColMap = new HashMap<String, HashMap<String, Object>>();

        while (fromColumnMeta.next()) {
            HashMap<String, Object> fromMap = new HashMap<String, Object>();
            fromMap.put("DATA_TYPE", fromColumnMeta.getInt("DATA_TYPE"));
            fromMap.put("TYPE_NAME", fromColumnMeta.getString("TYPE_NAME"));
            fromMap.put("COLUMN_SIZE", fromColumnMeta.getInt("COLUMN_SIZE"));
            fromMap.put("DECIMAL_DIGITS", fromColumnMeta.getInt("DECIMAL_DIGITS"));
            String colName = fromColumnMeta.getString("COLUMN_NAME");
            fromColMap.put(colName.toUpperCase(), fromMap);
        }

        while (toColumnMeta.next()) {
            HashMap<String, Object> toMap = new HashMap<String, Object>();
            toMap.put("DATA_TYPE", toColumnMeta.getInt("DATA_TYPE"));
            toMap.put("TYPE_NAME", toColumnMeta.getString("TYPE_NAME"));
            toMap.put("COLUMN_SIZE", toColumnMeta.getInt("COLUMN_SIZE"));
            toMap.put("DECIMAL_DIGITS", toColumnMeta.getInt("DECIMAL_DIGITS"));
            String colName = toColumnMeta.getString("COLUMN_NAME");
            toColMap.put(colName.toUpperCase(), toMap);
        }

        Iterator<Entry<String, HashMap<String, Object>>> toIterator = toColMap.entrySet().iterator();
        Iterator<Entry<String, HashMap<String, Object>>> fromIterator = fromColMap.entrySet().iterator();

        while (toIterator.hasNext()) {
            // 以目标字段为基准进行检查
            Entry<String, HashMap<String, Object>> toEntry = toIterator.next();
            String toColName = toEntry.getKey();
            HashMap<String, Object> toMap = toEntry.getValue();
            HashMap<String, Object> fromMap = fromColMap.get(toColName);

            if (fromMap == null) {
                // 原表找不到字段，说明该字段被删除，删除的sql语句是通用的，执行完直接比较下一个
                sb.append(String.format("alter table %s.%s drop column %s ;", toSchema, toTable, toColName));
                continue;
            }
            // 当两者不一致时以原表为准。
            Integer colType = (Integer) fromMap.get("DATA_TYPE");
            if (colType == Types.CHAR || colType == Types.VARCHAR) {// 处理字符串类型
                Integer fromColLength = (Integer) fromMap.get("COLUMN_SIZE") * colSizeTimes.getTimes();
                Integer toColLength = (Integer) toMap.get("COLUMN_SIZE");
                if (fromColLength > toColLength) {
                    //针对不同数据库使用与之匹配的 sql 语句
                    if ("db2".equals(toType)) {
                        sb.append(String.format("alter table %s.%s alter column %s set data type varchar(%d);",
                                toSchema, toTable, toColName, fromColLength));
                    } else if ("mysql".equals(toType) || "oracle".equals(toType)) {
                        sb.append(String.format("alter table %s.%s modify column %s varchar(%d);", toSchema, toTable,
                                toColName, fromColLength));
                    } else if ("postgres".equals(toType)) {
                        sb.append(String.format("alter table %s.%s alter column %s type character varying(%d);",
                                toSchema, toTable, toColName, fromColLength));
                    } else{
                        sb.append(String.format("alter table %s.%s alter column %s type character varying(%d);",
                                toSchema, toTable, toColName, fromColLength));
                    }

                }
            } else if (colType == Types.DECIMAL) {// 处理小数字段类型
                Integer fromColLength = (Integer) fromMap.get("COLUMN_SIZE");
                Integer fromColDigitLength = (Integer) fromMap.get("DECIMAL_DIGITS");
                Integer toColLength = (Integer) toMap.get("COLUMN_SIZE");
                Integer toColDigitLength = (Integer) toMap.get("DECIMAL_DIGITS");

                if (fromColLength > toColLength || fromColDigitLength > toColDigitLength) {
                    if ("db2".equals(toType)) {
                        sb.append(String.format("alter table %s.%s alter column %s set data type decimal(%d,%d);",
                                toSchema, toTable, toColName, fromColLength, fromColDigitLength));
                    } else if ("mysql".equals(toType) || "oracle".equals(toType)) {
                        sb.append(String.format("alter table %s.%s modify column %s decimal(%d,%d);", toSchema, toTable,
                                toColName, fromColLength, fromColDigitLength));
                    } else if ("postgres".equals(toType)) {
                        sb.append(String.format("alter table %s.%s alter column %s type decimal(%d,%d);", toSchema,
                                toTable, toColName, fromColLength, fromColDigitLength));
                    } else{
                        sb.append(String.format("alter table %s.%s alter column %s type decimal(%d,%d);", toSchema,
                                toTable, toColName, fromColLength, fromColDigitLength));
                    }

                }
            }
        }

        // 自动增加字段，以原表为主
        while (fromIterator.hasNext()) {
            Entry<String, HashMap<String, Object>> fromEntry = fromIterator.next();
            String fromColName = fromEntry.getKey();
            HashMap<String, Object> fromMap = fromEntry.getValue();
            HashMap<String, Object> toMap = toColMap.get(fromColName);

            if (toMap == null) {
                String typeName = (String) fromMap.get("TYPE_NAME");
                Integer colType = (Integer) fromMap.get("DATA_TYPE");
                Integer fromColLength = (Integer) fromMap.get("COLUMN_SIZE");
                Integer fromColDigitLength = (Integer) fromMap.get("DECIMAL_DIGITS");
                switch (colType) {
                    case Types.CHAR:
                    case Types.VARCHAR:
                        sb.append(String.format("alter table %s.%s add column %s varchar(%d);", toSchema, toTable,
                                fromColName, fromColLength * colSizeTimes.getTimes()));
                        break;
                    case Types.DECIMAL:
                        sb.append(String.format("alter table %s.%s add column %s decimal(%d,%d);", toSchema, toTable,
                                fromColName, fromColLength, fromColDigitLength));
                        break;
                    default:
                        sb.append(String.format("alter table %s.%s add column %s %s;", toSchema, toTable, fromColName,
                                typeName));
                }
            }

        }
        return sb.toString();

    }

    public static void printHelp() {
        System.out.println(
                "Usage: \njava -jar database-sync-1.0.jar [options] {fromDB} {fromSchema} {fromTable} {toDB} {toSchema} {toTable} [whereClause]");
        System.out.println("options:");
        System.out.println("        -v or --version                            :print version then exit");
        System.out.println("        -h or --help                               :print help info then exit");
        System.out.println("        -sd or --sync-ddl                          :auto synchronize table structure");
        System.out.println("        -ff=col1,col2 or --from-fields=col1,col2   :specify from fields");
        System.out.println("        -tf=col3,col4 or --to-fields=col3,col4     :specify to fields");
        System.out.println("        --no-feature or -nf                        :will not use database's feature");

    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException {
        
        App app = new App();
        if(!app.handleCommandLineArgs(args)){
            return;
        }

        String currentPath = System.getProperty("user.dir");
        logger.info("current path: " + currentPath);
        logger.info(
                String.format("begin (%s)%s.%s -> (%s)%s.%s", app.getFromDBId(), app.getFromSchema(), app.getFromTable(), app.getToDbId(), app.getToSchema(), app.getToTable()));
        
        Config configFrom = Config.getInstance(app.getFromDBId());
        Config configTo = Config.getInstance(app.getToDbId());

        try {

            //读取都采用默认的 jdbc
            DataBaseSync fromDataBase = DataBaseSyncFactory.getInstance(configFrom, false);

            //写入的话，可以根据传入参数，决定是否优先使用数据库具体特性
            DataBaseSync toDataBase = DataBaseSyncFactory.getInstance(configTo,app.getIsSpecificFeture());

            List<String> fromColumnNames = new ArrayList<>();
            List<String> toColumnNames = new ArrayList<>();

            // 原表必须存在
            if (!fromDataBase.existsTable(app.getFromSchema(), app.getFromTable())) {
                logger.info(String.format("fromTable -> (%s) %s.%s not exists! Program exit.", app.getFromDBId(), app.getFromSchema(),
                        app.getFromTable()));
                return;
            }

            if (!app.getIsSyncTableDDL()) {
                // 如果不同步表结构，那么目标表必须存在
                if (!toDataBase.existsTable(app.getToSchema(), app.getToTable())) {
                    logger.info(
                        String.format("toTable -> (%s) %s.%s not exists! Program exit.", app.getToDbId(), app.getToSchema(), app.getToTable()));
                    return;
                }

                if(app.getToTableFields().size() > 0) {
                    // 如果指定目标列，则使用目标列，不指定则自动获取
                    toColumnNames = app.getToTableFields();
                } else {
                    toColumnNames = toDataBase.getTableColumns(app.getToSchema(), app.getToTable());
                }

                if (app.getFromTableFields().size() > 0) {
                    // 如果指定源列，则使用源列，不指定则使用目标列
                    fromColumnNames = app.getFromTableFields();
                } else {
                    fromColumnNames = toColumnNames;
                }

                ResultSet rs = fromDataBase.readData(app.getFromSchema(), app.getFromTable(), fromColumnNames, app.getWhereClause());
                toDataBase.writeData(app.getToSchema(), app.getToTable(), toColumnNames, rs, app.getWhereClause());
                logger.info(String.format("finished (%s)%s.%s -> (%s)%s.%s", app.getFromDBId(), app.getFromSchema(), app.getFromTable(), app.getToDbId(),
                        app.getToSchema(), app.getToTable()));
                return;
            }

            ColSizeTimes colTimes = "gbk".equals(configFrom.getEncoding()) && "utf-8".equals(configTo.getEncoding()) ? ColSizeTimes.DOUBLE : ColSizeTimes.EQUAL;

            if (colTimes != ColSizeTimes.EQUAL) {
                //提示字段长度扩大了
                logger.info(
                        String.format("The varchar column length of target table is %d times that of the source table",
                                colTimes.getTimes()));
            }

            if (!toDataBase.existsTable(app.getToSchema(), app.getToTable())) {
                // 目标表不存在，先创建
                logger.info(String.format("toTable -> (%s) %s.%s not exists, create it.", app.getToDbId(), app.getToSchema(), app.getToTable()));
                String ddl = fromDataBase.getDDL(app.getToDbId(), app.getFromSchema(), app.getFromTable(), colTimes);
                try {
                    toDataBase.createTable(app.getToSchema(), app.getToTable(), ddl);
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            } else {

                // 目标表存在，判断同名字段长度是否一致
                ResultSet fromColMeta = fromDataBase.getColMetaData(app.getFromSchema(),app.getFromTable());
                ResultSet toColMeta = toDataBase.getColMetaData(app.toSchema, app.toTable);
                /* 同步扩充字段长度 */
                String alterSql = getAlterSqlFromMetaData(configTo.getDbType(),app.toSchema, app.toTable, fromColMeta, toColMeta, colTimes);
                String[] alterSqlArray = alterSql.split(";");
                for (int i = alterSqlArray.length - 1; i >= 0; i--) {
                    String sql = alterSqlArray[i];
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

            if (app.getToTableFields().size() > 0) {
                // 如果指定目标列，则使用目标列，不指定则自动获取
                toColumnNames = app.getToTableFields();
            } else {
                toColumnNames = toDataBase.getTableColumns(app.getToSchema(),app.getToTable());
            }

            if (app.getFromTableFields().size() > 0) {
                // 如果指定源列，则使用源列，不指定则 使用目标列
                fromColumnNames = app.getFromTableFields();
            } else {
                fromColumnNames = toColumnNames;
            }

            ResultSet rs = fromDataBase.readData(app.getFromSchema(), app.getFromTable(), fromColumnNames, app.getWhereClause());
            toDataBase.writeData(app.getToSchema(), app.getToTable(), toColumnNames, rs, app.getWhereClause());
            logger.info(String.format("finished (%s)%s.%s -> (%s)%s.%s", app.getFromDBId(), app.getFromSchema(), app.getFromTable(), app.getToDbId(), app.getToSchema(),
                    app.getToTable()));
        } finally {


        }
    }
}

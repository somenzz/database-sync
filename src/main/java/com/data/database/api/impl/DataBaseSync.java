package com.data.database.api.impl;

import com.data.database.api.DataSync;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Types;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.DatabaseMetaData;

import java.util.List;
import java.util.Objects;
import java.util.ArrayList;
import com.google.common.base.Joiner;
import com.data.database.utils.Tools;
import com.data.database.api.MyEnum.ColSizeTimes;

import org.apache.log4j.Logger;

public class DataBaseSync implements DataSync {

    /*
     * 获取一个表的列名，用于 readData 时指定一个表的字段，之所以不使用 * 是因为源数据库表的字段有可能新增，
     * 也可以用于表结构的自动同步：当目标表的字段数少于源表时，对目标表自动执行 alter table add columns xxx
     */
    protected final String jdbcDriver;
    protected final String dbUrl;
    // 数据库的用户名与密码，需要根据自己的设置
    protected final String dbUser;
    protected final String dbPass;
    protected Connection dbConn;
    protected final String dbType;
    protected final Logger logger = Logger.getLogger(DataBaseSync.class);
    protected final Integer bufferRows;

    public DataBaseSync(final String dbType, final String jdbcDriver, final String dbUrl, final String dbUser,
            final String dbPass, final Integer bufferRows) throws SQLException, ClassNotFoundException {
        this.dbType = dbType;
        this.jdbcDriver = jdbcDriver;
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPass = dbPass;
        Class.forName(this.jdbcDriver);
        if (this.jdbcDriver.equals("com.ibm.db2.jcc.DB2Driver")) {
            System.setProperty("db2.jcc.charsetDecoderEncoder", "3");
        }
        this.dbConn = DriverManager.getConnection(this.dbUrl, this.dbUser, this.dbPass);
        if ("oracle.jdbc.driver.OracleDriver".equals(this.jdbcDriver)) {
            // 设置oracle的事务级别
        } else {
            // oracle 不支持脏读
            this.dbConn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);// 读未提交
        }
        this.bufferRows = bufferRows;
        String ipAndPort = this.dbUrl.split("/")[2];
        logger.info(ipAndPort + " connection establied.");
    }

    public String convertColumnType(String dbType, String colType) {
        if (dbType.equals("postgres") || dbType.equals("elk")) {
            return colType.toUpperCase().replace("CLOB", "TEXT").replace("VARCHAR2", "VARCHAR");
        } else if (dbType.equals("db2") || dbType.equals("edw")) {
            return colType.toUpperCase().replace("INT4", "INTEGER").replace("TEXT", "CLOB").replace("SERIAL", "INTEGER")
                    .replace("VARCHAR2", "VARCHAR");
        } else if (dbType.equals("oracle")) {
            return colType.toUpperCase().replace("VARCHAR", "VARCHAR2");
        }
        return colType;
    }

    public ResultSet getColMetaData(String schemaName, String tableName) throws SQLException {
        // to do 如果源表与目标表不一致，则以愿表为准，修改目标表：
        // 用于 扩字段长度，增加字段，删除字段，修改字段类型。
        if (this.dbType.equals("postgres") || this.dbType.equals("elk")) {
            if (schemaName != null) {
                schemaName = schemaName.toLowerCase();
            }
            if (tableName != null) {
                tableName = tableName.toLowerCase();
            }
        } else {
            schemaName = schemaName == null ? null : schemaName.toUpperCase();
            tableName = tableName == null ? null : tableName.toUpperCase();
        }

        String tableCatalog = null;
        if (this.dbType.equals("mysql")) {
            tableCatalog = schemaName;
        }
        DatabaseMetaData metaData = this.dbConn.getMetaData();
        ResultSet resultSet = metaData.getColumns(tableCatalog, schemaName, tableName, null);
        return resultSet;

    }

    public List<Integer> getColumnTypes(String schemaName, String tableName) throws SQLException {
        List<Integer> colTypes = new ArrayList<>();
        if (this.dbType.equals("postgres") || this.dbType.equals("elk")) {
            if (schemaName != null) {
                schemaName = schemaName.toLowerCase();
            }
            if (tableName != null) {
                tableName = tableName.toLowerCase();
            }
        } else {
            schemaName = schemaName == null ? null : schemaName.toUpperCase();
            tableName = tableName == null ? null : tableName.toUpperCase();
        }
        // System.out.println(metaData.getCatalogTerm());
        // ResultSet catalogs = metaData.getCatalogs();
        // while (catalogs.next()) {
        // String catalog = catalogs.getString(1); // "TABLE_CATALOG"
        // System.out.println("catalog: " + catalog);
        // }
        String tableCatalog = null;
        if (this.dbType.equals("mysql")) {
            tableCatalog = schemaName;
        }
        DatabaseMetaData metaData = this.dbConn.getMetaData();
        ResultSet resultSet = metaData.getColumns(tableCatalog, schemaName, tableName, null);
        while (resultSet.next()) {
            colTypes.add(resultSet.getInt(5));
        }
        return colTypes;

    }

    public List<String> getTableColumns(String schemaName, String tableName) throws SQLException {
        logger.info(String.format("get table columns from %s.%s ", schemaName, tableName));
        List<String> columnNames = new ArrayList<>();
        DatabaseMetaData metaData = this.dbConn.getMetaData();

        if (this.dbType.equals("postgres") || this.dbType.equals("elk")) {
            if (schemaName != null) {
                schemaName = schemaName.toLowerCase();
            }
            if (tableName != null) {
                tableName = tableName.toLowerCase();
            }
        } else {
            schemaName = schemaName == null ? null : schemaName.toUpperCase();
            tableName = tableName == null ? null : tableName.toUpperCase();
        }
        // System.out.println(metaData.getCatalogTerm());
        // ResultSet catalogs = metaData.getCatalogs();
        // while (catalogs.next()) {
        // String catalog = catalogs.getString(1); // "TABLE_CATALOG"
        // System.out.println("catalog: " + catalog);
        // }
        String tableCatalog = null;
        if (this.dbType.equals("mysql")) {
            tableCatalog = schemaName;
        }
        ResultSet resultSet = metaData.getColumns(tableCatalog, schemaName, tableName, null);
        while (resultSet.next()) {
            // for(int i =0 ;i < 24; i++){
            // System.out.print(resultSet.getObject(i+1)+"\t");
            // }
            // System.out.println("");
            columnNames.add(resultSet.getString(4));
        }
        return columnNames;

    };

    /* 判断一个表是否存在 */
    public boolean existsTable(String schemaName, String tableName) throws SQLException {
        DatabaseMetaData metaData = this.dbConn.getMetaData();
        if (this.dbType.equals("postgres") || this.dbType.equals("elk")) {
            if (schemaName != null) {
                schemaName = schemaName.toLowerCase();
            }
            if (tableName != null) {
                tableName = tableName.toLowerCase();
            }
        } else {
            schemaName = schemaName == null ? null : schemaName.toUpperCase();
            tableName = tableName == null ? null : tableName.toUpperCase();
        }

        ResultSet resultSet = metaData.getTables(null, schemaName, tableName, null);
        if (resultSet.next()) {
            return true;
        }
        return false;

    };

    /* 获取一个表的 DDL 语句，用于在目标数据库创建表 lenSize 默认值 1 */
    public String getDDL(String dbType, String schemaName, String tableName, ColSizeTimes lenSize) throws SQLException {

        DatabaseMetaData metaData = this.dbConn.getMetaData();
        // ResultSet tables = metaData.getTables(null, schemaName, null, null);
        // while (tables.next()) {
        // System.out.println(String.format("%s.%s", tables.getString(2),
        // tables.getString(3)));
        // }

        StringBuilder sb = new StringBuilder(1024);
        StringBuilder sb_remark = new StringBuilder(1024);
        String table_remark = "";

        String tableCatalog = null;
        if (this.dbType.equals("mysql") || this.dbType.equals("postgres") || this.dbType.equals("elk")) {
            tableCatalog = schemaName;
            schemaName = schemaName == null ? null : schemaName.toLowerCase();
            tableName = tableName == null ? null : tableName.toLowerCase();
        } else {
            schemaName = schemaName == null ? null : schemaName.toUpperCase().replace(" ", "");
            tableName = tableName == null ? null : tableName.toUpperCase();
        }

        ResultSet tbResultset = metaData.getTables(tableCatalog, schemaName, tableName, null);
        if (tbResultset.next()) {
            table_remark = tbResultset.getString(5);
        }

        ResultSet resultSet = metaData.getColumns(tableCatalog, schemaName, tableName, null);

        int i = 0;
        ArrayList<String> pks = new ArrayList<String>();
        ResultSet resultSetPKS = metaData.getPrimaryKeys(null, schemaName, tableName);
        while (resultSetPKS.next()) {
            pks.add(resultSetPKS.getString(4));
        }

        sb.append("create table #tabname#");
        while (resultSet.next()) {
            i++;
            String colunmName = resultSet.getString(4);
            Integer dataType = resultSet.getInt(5);
            String columnType = convertColumnType(dbType, resultSet.getString(6));
            Integer columnSize = resultSet.getInt(7);
            Integer digitSize = resultSet.getInt(9);
            String remark = resultSet.getString(12);
            String columnTypeText = "";
            if (columnType.equals("TEXT") || columnType.equals("CLOB") || columnType.equals("BLOB")) {
                columnTypeText = columnType;
            } else if (dataType == Types.VARCHAR || dataType == Types.CHAR) {
                // varchar || char
                columnSize = columnSize * lenSize.getTimes();
                columnTypeText = columnType + "(" + columnSize + ")";
            } else if (dataType == Types.DECIMAL) {
                columnTypeText = columnType + "(" + columnSize + "," + digitSize + ")";
            } else {
                columnTypeText = columnType;
            }

            if (i == 1) {
                sb.append("(\n");
                sb.append(colunmName);
                sb.append("  ");
                sb.append(columnTypeText);
            } else {
                sb.append(",");
                sb.append(colunmName);
                sb.append("  ");
                sb.append(columnTypeText);
            }
            // 如果是主键，则不能为空
            if (pks.contains(colunmName)) {
                sb.append(" not null");
            }

            if (remark != null) {
                remark = remark.replace(";", "|").replace("'", "");
                if (!dbType.equals("mysql")) {
                    sb_remark.append("comment on column #tabname#.");
                    sb_remark.append(colunmName);
                    sb_remark.append(" is '" + remark + "';\n");
                } else {
                    // mysql的注释比较特别
                    sb.append(" comment '" + remark + "'");
                }
            }

            sb.append(" \n");
        }
        sb.append(")");

        // 添加 edw 的表空间信息
        if (dbType.equals("edw")) {
            sb.append("IN TBS_IPD_DATA\nINDEX IN TBS_IPD_INDEX\n");
        }

        if (dbType.equals("elk")) {
            sb.append("WITH(compression=no,orientation=orc,version=0.12)\ntablespace hdfs\n");
        }

        // 添加主键或分区键信息
        if (pks.size() > 0) {
            if (dbType.equals("elk")) {
                sb.append("distribute by hash(" + Joiner.on(",").join(pks) + ")");
            } else {
                sb.append(";\n");
                sb.append("alter table #tabname# add constraint pk_#pk_name# primary key (" + Joiner.on(",").join(pks)
                        + ")");
            }
        }

        // 统一添加分号结尾
        sb.append(";\n");
        // 添加表注释信息
        if (table_remark != null && !table_remark.isEmpty()) {
            table_remark = table_remark.replace(";", "").replace("'", "");
            if (dbType.equals("mysql")) {
                sb.append("alter table #tabname# comment '" + table_remark + "';\n");
            } else {
                sb.append("comment on table #tabname# is '" + table_remark + "';\n");
            }

        }
        // 添加字段注释信息
        sb.append(sb_remark.toString());
        String ddl = sb.toString();
        logger.info(String.format("get ddl from  %s.%s ", schemaName, tableName));
        return ddl;

    };

    /* 获取一个表的数据，采用流式读取，可以提供 whereClause 表示增量读取 ，如果 whereClause 为空，表示全量读取 */
    public ResultSet readData(String schemaName, final String tableName, final List<String> columnNames,
            String whereClause) throws SQLException {
        this.dbConn.setAutoCommit(false);
        logger.info(String.format("read data from  %s.%s ", schemaName, tableName));
        final String columns = Joiner.on(", ").join(columnNames);
        String selectSql = "";
        if (whereClause == null || whereClause.isEmpty()) {
            selectSql = "select " + columns + " from " + schemaName + "." + tableName;
        } else {
            selectSql = "select " + columns + " from " + schemaName + "." + tableName + " where " + whereClause;
        }
        logger.info(selectSql);
        PreparedStatement pStemt = null;
        pStemt = this.dbConn.prepareStatement(selectSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        pStemt.setFetchSize(bufferRows + 10000);
        final ResultSet rs = pStemt.executeQuery();
        return rs;

    };

    /* 将 ResultSet 类型的数据定入一张表，写入成功返回 true,否则返回 false */
    public boolean writeData(String schemaName, final String tableName, final List<String> columnNames, ResultSet rs,
            final String whereClause) throws IOException, SQLException {
        this.dbConn.setAutoCommit(false);
        logger.info(String.format("begin insert into %s.%s...", schemaName, tableName));
        String tbName = "";
        String clearSql = "";
        if (schemaName != null && schemaName.length() != 0) {
            tbName = schemaName + "." + tableName;
        } else {
            tbName = tableName;
        }
        if (whereClause != null && !whereClause.isEmpty()) {
            // 如果 whereClause 不为空
            clearSql = "delete from " + tbName + " where " + whereClause;
        } else {
            // 如果 whereClause 为空
            clearSql = "truncate table " + tbName;
            if (dbType.equals("db2") || dbType.equals("edw")) {
                clearSql = clearSql + " immediate";
            } else if (dbType.equals("elk")) {
                clearSql = "delete from " + tbName;
            }
            // 如果是 elk 请修改为 delete from
        }

        logger.info(String.format("clear data for %s is begin...", tbName));
        logger.info("clearSql : " + clearSql);
        this.dbConn.commit();
        PreparedStatement pStemt = this.dbConn.prepareStatement(clearSql);
        pStemt.executeUpdate();
        this.dbConn.commit();
        logger.info(String.format("clear data for %s is done", tbName));

        List<Integer> colTypes = this.getColumnTypes(schemaName, tableName);

        final String insertSql = Tools.buildInsertSql(tbName, columnNames.stream().toArray(String[]::new));
        // System.out.println(insertSql);
        pStemt = this.dbConn.prepareStatement(insertSql);
        // final int numberOfCols = rs.getMetaData().getColumnCount();
        final int numberOfCols = columnNames.size();
        int rowCount = 0;
        long totalAffectRows = 0;
        long starttime = System.currentTimeMillis();
        while (rs.next()) {
            for (int i = 0; i < numberOfCols; i++) {
                if (colTypes.get(i) == Types.VARCHAR || colTypes.get(i) == Types.CHAR
                        || colTypes.get(i) == Types.CLOB) {
                    pStemt.setString(i + 1, Objects.toString(rs.getString(i + 1), "")); // 将 null 转化为空
                } else {
                    pStemt.setObject(i + 1, rs.getObject(i + 1));
                }
            }
            pStemt.addBatch();
            rowCount++;
            if (rowCount >= bufferRows) {
                // 每10万行提交一次记录
                int affectRows = 0;
                for (int i : pStemt.executeBatch()) {
                    affectRows += i;
                }
                this.dbConn.commit();
                logger.info(String.format("rows insert into %s is %d", tbName, affectRows));
                totalAffectRows += affectRows;
                rowCount = 0;
            }
        }
        // 处理剩余的记录
        int affectRows = 0;
        for (int i : pStemt.executeBatch()) {
            affectRows += i;
        }
        this.dbConn.commit();
        logger.info(String.format("rows insert into %s is %d", tbName, affectRows));
        totalAffectRows += affectRows;
        rowCount = 0;
        long endtime = System.currentTimeMillis();
        logger.info(
                String.format("insert into %s %d rows is done. cost %.3f seconds", tbName,totalAffectRows,(endtime - starttime) * 1.0 / 1000));
        return true;
    };

    /* 执行提供的 ddl 建表 */
    public boolean createTable(String schemaName, String tableName, String ddl) throws SQLException {

        this.dbConn.setAutoCommit(true);
        String newDDL = "";
        if (schemaName == null || schemaName.length() == 0) {
            newDDL = ddl.replace("#tabname#", tableName);
        } else {
            // 针对 postgres 如果模式名不存在，先创建模式名，否则创建表失败。
            newDDL = ddl.replace("#tabname#", schemaName + "." + tableName);
        }
        newDDL = newDDL.replace("#pk_name#", tableName);
        logger.info(newDDL);
        String[] sqls = newDDL.split(";");
        for (String sql : sqls) {
            if (!sql.isEmpty()) {
                PreparedStatement pStemt = this.dbConn.prepareStatement(sql);
                pStemt.executeUpdate();
            }
        }
        return true;

    };

    public boolean executeUpdateSQL(String sql) throws SQLException {
        PreparedStatement pStemt = this.dbConn.prepareStatement(sql);
        pStemt.executeUpdate();
        this.dbConn.commit();
        return true;
    }

}
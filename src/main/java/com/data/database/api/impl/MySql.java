package com.data.database.api.impl;

import com.data.database.api.DataSync;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;

import java.util.List;
import java.util.ArrayList;
import com.google.common.base.Joiner;
import com.data.database.utils.Tools;

public class MySql implements DataSync {

    /*
     * 获取一个表的列名，用于 readData 时指定一个表的字段，之所以不使用 * 是因为源数据库表的字段有可能新增，
     * 也可以用于表结构的自动同步：当目标表的字段数少于源表时，对目标表自动执行 alter table add columns xxx
     */
    private final String jdbcDriver;
    private final String dbUrl;
    // 数据库的用户名与密码，需要根据自己的设置
    private final String dbUser;
    private final String dbPass;
    private final Connection dbConn;

    public MySql(final String jdbcDriver, final String dbUrl, final String dbUser, final String dbPass)
            throws SQLException {
        this.jdbcDriver = jdbcDriver;
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPass = dbPass;
        // this.JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
        // this.DB_URL =
        // "jdbc:mysql://localhost:3306/aarondb?useSSL=false&characterEncoding=utf8&serverTimezone=UTC";
        // 数据库的用户名与密码，需要根据自己的设置
        // this.USER = "aaron";
        // this.PASS = "aaron";

        try {
            Class.forName(this.jdbcDriver);
        } catch (final ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        this.dbConn = DriverManager.getConnection(this.dbUrl, this.dbUser, this.dbPass);
    }

    public List<String> getTableColumns(String schemaName,final String tableName) {
        System.out.println("getTableColumns");
        final List<String> columnNames = new ArrayList<>();
        try {
            final PreparedStatement pStemt = this.dbConn.prepareStatement(
                    "select COLUMN_NAME from information_schema.columns where TABLE_SCHEMA= ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION");
            pStemt.setObject(1, schemaName);
            pStemt.setObject(2, tableName);
            try (ResultSet rs = pStemt.executeQuery()) {
                while (rs.next()) {
                    columnNames.add(rs.getString(1));
                }
            }
        } catch (final SQLException e) {
            System.out.println("getColumnNames failure: " + e);
        } finally {
            System.out.println("getColumnNames finally");
        }
        return columnNames;

    };

    /* 判断一个表是否存在 */
    public boolean existsTable(String schemaName, final String tableName) {
        System.out.println("existsTable");

        // select count(1) from information_schema.tables where TABLE_NAME = 'user' and
        // table_schema = 'kjt'

        try {
            final PreparedStatement pStemt = this.dbConn
                    .prepareStatement("select count(1) from information_schema.tables where TABLE_SCHEMA= ? AND  TABLE_NAME = ? ");
            pStemt.setObject(1, schemaName);
            pStemt.setObject(2, tableName);
            try (ResultSet rs = pStemt.executeQuery()) {
                while (rs.next()) {
                    if (rs.getInt(1) > 0) {
                        return true;
                    }
                    return false;
                }
            }
        } catch (final SQLException e) {
            System.out.println("getColumnNames failure: " + e);
        } finally {
            System.out.println("getColumnNames finally");
        }
        return true;

    };

    /* 获取一个表的 DDL 语句，用于在目标数据库创建表 lenSize 默认值 1*/
    public String getDDL(String dbType, String schemaName,final String tableName, int lenSize) throws SQLException {
        assert(lenSize >= 1 && lenSize <= 3);
        System.out.println("getDDL");

        StringBuilder sb = new StringBuilder(1024);
        sb.append("create table " + tableName + "(\n");
        final PreparedStatement pStemt = this.dbConn.prepareStatement(
                "select COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,COLUMN_TYPE from information_schema.columns where TABLE_SCHEMA = ? AND  TABLE_NAME = ? ORDER BY ORDINAL_POSITION");
        pStemt.setString(1, schemaName);
        pStemt.setString(2, tableName);
        final ResultSet rs = pStemt.executeQuery();
        while (rs.next()){
            String colunmName = rs.getString(1);
            String dataType = rs.getString(2);
            String columnType = rs.getString(4);
            if (dataType == "varchar" || dataType == "char"){
                int columnLength = rs.getInt(3);
                columnType = "varchar(" + columnLength*lenSize + ")";
            }
            sb.append(colunmName);
            sb.append(' ');
            sb.append(columnType);
            sb.append(", \n");
        }
        final String ddl = sb.toString();
        return ddl;

    };

    /* 获取一个表的数据，采用流式读取，可以提供 whereClause 表示增量读取 ，如果 whereClause 为空，表示全量读取 */
    public ResultSet readData(String schemaName,final String tableName, final List<String> columnNames, String whereClause)
            throws SQLException {
        System.out.println("readData");
        final String columns = Joiner.on(", ").join(columnNames);
        if (whereClause == null) {
            whereClause = "1 = 1";
        }
        final String selectSql = "select " + columns + " from " + schemaName + "." + tableName + " where " + whereClause;
        System.out.println(selectSql);
        PreparedStatement pStemt = null;
        pStemt = this.dbConn.prepareStatement(selectSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        pStemt.setFetchSize(Integer.MIN_VALUE);
        final ResultSet rs = pStemt.executeQuery();
        return rs;

    };

    /* 将 ResultSet 类型的数据定入一张表，写入成功返回 true,否则返回 false */
    public boolean writeData(String schemaName, final String tableName, final List<String> columnNames, final ResultSet rs,
            final String whereClause) throws SQLException {
        System.out.println("writeData");

        String clearSql = "";
        String tbName = schemaName + "." + tableName;
        if (whereClause != null) {
            clearSql = "delete from " + tbName + " where " + whereClause;
        } else {
            clearSql = "truncate table " + tbName;
        }

        PreparedStatement pStemt = this.dbConn.prepareStatement(clearSql);
        pStemt.executeUpdate();
        System.out.println("数据清理完毕。");

        final String insertSql = Tools.buildInsertSql(tbName, columnNames.stream().toArray(String[]::new));
        // System.out.println(insertSql);
        pStemt = this.dbConn.prepareStatement(insertSql);
        final int numberOfcols = rs.getMetaData().getColumnCount();
        int rowCount = 0;
        while (rs.next()) {
            for (int i = 0; i < numberOfcols; i++) {
                pStemt.setObject(i + 1, rs.getString(i + 1));
            }
            pStemt.addBatch();
            rowCount++;
            if (rowCount >= 100) {
                // 每10万行提交一次记录
                int affectRows = 0;
                for (int i : pStemt.executeBatch()) {
                    affectRows += i;
                }
                System.out.println("Rows inserted into table " + tbName + ": " + affectRows);
                rowCount = 0;
            }
        }
        // 处理剩余的记录
        int affectRows = 0;
        for (int i : pStemt.executeBatch()) {
            affectRows += i;
        }
        System.out.println("Rows inserted into table " + tbName + ": " + affectRows);
        rowCount = 0;

        System.out.println("数据插入完毕。");
        return true;
    };

    /* 执行提供的 ddl 建表 */
    public boolean createTable(String schemaName, final String tableName, final String ddl) throws SQLException {
        System.out.println("createTable");
        final PreparedStatement pStemt = this.dbConn.prepareStatement(ddl);
        final int number = pStemt.executeUpdate();
        System.out.println(number);
        return true;

    };

}
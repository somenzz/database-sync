package com.data.database.api.impl;

import com.data.database.api.DataSync;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;

import java.util.List;
import java.util.ArrayList;

public class MySql implements DataSync {

    /*
     * 获取一个表的列名，用于 readData 时指定一个表的字段，之所以不使用 * 是因为源数据库表的字段有可能新增，
     * 也可以用于表结构的自动同步：当目标表的字段数少于源表时，对目标表自动执行 alter table add columns xxx
     */
    public List<String> getTableColumns(Connection conn, String tableName) throws SQLException {
        System.out.println("getTableColumns");
        List<String> columnNames = new ArrayList<>();
        // try {
        // PreparedStatement pStemt = conn.prepareStatement("select * from " + tableName
        // + " limit 1");
        // // 结果集元数据
        // ResultSetMetaData rsmd = pStemt.getMetaData();
        // // 表列数
        // int size = rsmd.getColumnCount();
        // for (int i = 0; i < size; i++) {
        // columnNames.add(rsmd.getColumnName(i + 1));
        // }
        // } catch (SQLException e) {
        // System.out.println("getColumnNames failure: " + e);
        // } finally {
        // System.out.println("getColumnNames finally");
        // }

        //select COLUMN_NAME from information_schema.columns where TABLE_NAME = 'user' and table_schema = 'kjt' ORDER BY ORDINAL_POSITION;
        try {
            PreparedStatement pStemt = conn.prepareStatement(
                    "select COLUMN_NAME from information_schema.columns where TABLE_NAME = ? ORDER BY ORDINAL_POSITION");
            pStemt.setObject(1, tableName);
            try (ResultSet rs = pStemt.executeQuery()) {
                while (rs.next()) {
                    columnNames.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            System.out.println("getColumnNames failure: " + e);
        } finally {
            System.out.println("getColumnNames finally");
        }
        return columnNames;

    };

    /* 判断一个表是否存在 */
    public boolean existsTable(Connection conn, String tableName){
        System.out.println("existsTable");

        //select count(1) from information_schema.tables where TABLE_NAME = 'user' and table_schema = 'kjt' 

        try {
            PreparedStatement pStemt = conn.prepareStatement(
                    "select count(1) from information_schema.tables where TABLE_NAME = ? ");
            pStemt.setObject(1, tableName);
            try (ResultSet rs = pStemt.executeQuery()) {
                while (rs.next()) {
                    if (rs.getInt(1)>0){
                        return true;
                    }
                    return false;
                }
            }
        } catch (SQLException e) {
            System.out.println("getColumnNames failure: " + e);
        } finally {
            System.out.println("getColumnNames finally");
        }
        return true;

    };

    /* 获取一个表的 DDL 语句，用于在目标数据库创建表 */
    public String getDDL(Connection conn, String tableName) throws SQLException {

        System.out.println("getDDL");
        return "create table";

    };

    /* 获取一个表的数据，采用流式读取，可以提供 whereClause 表示增量读取 ，如果 whereClause 为空，表示全量读取 */
    public ResultSet readData(Connection conn, String tableName, List<String> columnNames, String whereClause)
            throws SQLException {
        System.out.println("readData");
        PreparedStatement pStemt = conn.prepareStatement("select columnNames from tableName", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        pStemt.setFetchSize(Integer.MIN_VALUE);
        ResultSet rs = pStemt.executeQuery();
        return rs;

    };

    /* 将 ResultSet 类型的数据定入一张表，写入成功返回 true,否则返回 false */
    public boolean writeData(Connection conn, String tableName, ResultSet rs) throws SQLException {
        System.out.println("writeData");
        return true;

    };

    /* 执行提供的 ddl 建表 */
    public boolean createTable(Connection conn, String tableName, String ddl) throws SQLException {
        System.out.println("createTable");
        return true;

    };

}
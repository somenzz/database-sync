package com.data.database.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;

import java.util.List;


/*
数据库表同步接口
*/
public interface DataSync {

    /*
    获取一个表的列名，用于 readData 时指定一个表的字段，之所以不使用 * 是因为源数据库表的字段有可能新增，
    也可以用于表结构的自动同步：当目标表的字段数少于源表时，对目标表自动执行 alter table add columns xxx
    */
    public List<String> getTableColumns(Connection conn,String tableName) throws SQLException;

    /*判断一个表是否存在*/
    public boolean existsTable(Connection conn,String tableName) throws SQLException;

    /*获取一个表的 DDL 语句，用于在目标数据库创建表*/
    public String getDDL(Connection conn,String tableName) throws SQLException;

    /*获取一个表的数据，采用流式读取，可以提供 whereClause 表示增量读取 ，如果 whereClause 为空，表示全量读取*/
    public ResultSet readData(Connection conn,String tableName,List<String> columnNames,String whereClause) throws SQLException;

    /* 将 ResultSet 类型的数据定入一张表，写入成功返回 true,否则返回 false*/
    public boolean writeData(Connection conn,String tableName,ResultSet rs) throws SQLException;

    /* 执行提供的 ddl 建表*/
    public boolean createTable(Connection conn,String tableName,String ddl) throws SQLException;

}
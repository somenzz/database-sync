package com.data.database.api.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class PostgresDataBaseSync extends DataBaseSync {

    public PostgresDataBaseSync(String dbType, String jdbcDriver, String dbUrl, String dbUser, String dbPass,
            Integer bufferRows) throws SQLException, ClassNotFoundException {
        super(dbType, jdbcDriver, dbUrl, dbUser, dbPass, bufferRows);
    }

    @Override
    public boolean writeData(String schemaName, final String tableName, List<String> columnNames, ResultSet rs,
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

        if (whereClause != null && whereClause.length() != 0) {
            clearSql = "delete from " + tbName + " where " + whereClause;
        } else {
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

        // List<Integer> colTypes = this.getColumnTypes(schemaName, tableName);
        // final int numberOfcols = rs.getMetaData().getColumnCount();
        final int numberOfCols = columnNames.size();
        logger.info("numberOfcols = "+numberOfCols);
        // String[] rowArray = new String[numberOfcols];
        int rowCount = 0;
        long totalAffectRows = 0;
        StringBuilder sBuilder = new StringBuilder(bufferRows);
        String copyFromSql = String.format("copy %s (%s) from stdin csv delimiter e'\\x02' quote e'\\x03' escape e'\\x03' ",
                tbName,String.join(",",columnNames));
        logger.info("copyFromSql = "+copyFromSql);
        long starttime = System.currentTimeMillis();
        while (rs.next()) {
            for (int i = 0; i < numberOfCols - 1; i++) {
                String col = rs.getString(i+1);
                if (col != null) {
                    //去除字符串中最后的\\
                    int col_len = col.length();
                    while (col_len > 0 && col.charAt(col_len - 1) == '\\') {
                        col_len-- ;
                    }
                    sBuilder.append((char) 3).append(col.substring(0, col_len)).append((char) 3);
                } else {
                    sBuilder.append("");
                }
                sBuilder.append((char) 2);
            }
            String col = rs.getString(numberOfCols);
            if (col != null) {
                int col_len = col.length();
                while (col_len > 0 && col.charAt(col_len - 1) == '\\') {
                    col_len-- ;
                }
                sBuilder.append((char) 3).append(col.substring(0, col_len)).append((char) 3);
            } else {
                sBuilder.append("");
            }
            sBuilder.append('\n');
            rowCount++;
            if (rowCount >= bufferRows) {
                // 每10万行提交一次记录
                CopyManager copyManager = new CopyManager((BaseConnection) this.dbConn);
                // long rowsInserted = copyManager.copyIn("copy " + tbName + " from stdin
                // delimiter e'\\x02' NULL as ''",
                long rowsInserted = copyManager.copyIn(copyFromSql,
                        new BufferedReader(new StringReader(sBuilder.toString())));
                this.dbConn.commit();
                // sBuilder.delete(0, sBuilder.length());//清空缓冲区
                sBuilder.setLength(0);// 清空缓冲区
                logger.info(String.format("rows insert into %s is %d", tbName, rowsInserted));
                totalAffectRows += rowsInserted;
                rowCount = 0;
            }
        }
        // 处理剩余的记录

        CopyManager copyManager = new CopyManager((BaseConnection) this.dbConn);
        long rowsInserted = copyManager.copyIn(copyFromSql, new BufferedReader(new StringReader(sBuilder.toString())));
        this.dbConn.commit();
        sBuilder.setLength(0);// 清空缓冲区
        logger.info(String.format("rows insert into %s is %d", tbName, rowsInserted));
        totalAffectRows += rowsInserted;
        this.dbConn.setAutoCommit(true);
        long endtime = System.currentTimeMillis();
        logger.info(
                String.format("insert into %s %d rows has been completed, cost %.3f seconds", tbName, totalAffectRows,(endtime - starttime) * 1.0 / 1000));
        return true;
    };

}
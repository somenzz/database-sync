package com.data.database.api.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


public class OracleDataBaseSync extends DataBaseSync {
    //在此编写db2特性的代码逻辑


    public OracleDataBaseSync(String dbType, String jdbcDriver, String dbUrl, String dbUser, String dbPass,
            Integer bufferRows) throws SQLException, ClassNotFoundException {
        super(dbType, jdbcDriver, dbUrl, dbUser, dbPass, bufferRows);
    }


    // @Override
    // public boolean writeData(String schemaName, final String tableName, List<String> columnNames, ResultSet rs,
    //         final String whereClause)  {
    //     //如果 oracle 有更好的写入方法，在此实现

    //     return true;

    // }
};
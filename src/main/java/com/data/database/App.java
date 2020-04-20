package com.data.database;

import java.io.File;
import java.io.Reader;
import java.sql.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.data.database.api.DataSync;
import com.data.database.api.impl.DataBaseSync;
import java.util.List;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.data.database.api.MyEnum.ColSizeTimes;

/**
 * Hello world!
 */
public final class App {

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
     * 读取json文件，返回json串
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

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        // System.out.println(System.getProperty("user.dir"));
        JSONObject jobj = JSON.parseObject(readJsonFile("config.json"));
        String fromJDBC_DRIVER = jobj.getJSONObject("wbsj").getString("driver");
        String fromDB_URL = jobj.getJSONObject("wbsj").getString("url");
        String fromUSER = jobj.getJSONObject("wbsj").getString("user");
        String fromPASS = jobj.getJSONObject("wbsj").getString("password");
        String fromType = jobj.getJSONObject("wbsj").getString("type");

        String toJDBC_DRIVER = jobj.getJSONObject("postgres").getString("driver");
        String toDB_URL = jobj.getJSONObject("postgres").getString("url");
        String toUSER = jobj.getJSONObject("postgres").getString("user");
        String toPASS = jobj.getJSONObject("postgres").getString("password");
        String toType = jobj.getJSONObject("postgres").getString("type");

        try {
            DataSync fromDataBase = new DataBaseSync(fromType,fromJDBC_DRIVER, fromDB_URL, fromUSER, fromPASS);
            DataSync toDataBase = new DataBaseSync(toType,toJDBC_DRIVER, toDB_URL, toUSER, toPASS);
            // List<String> cols = ds.getTableColumns("SYSCAT", "TABLES");
            // String ddl = ds.getDDL("mysql","apidb","user",ColSizeTimes.DOUBLE);
            // for (String col : cols) {
            // System.out.println(col);
            // }
            String schemaName = "WBSJ";
            String tableName = "TD_RISKDETAIL";
            String whereClause = null;

            if (!toDataBase.existsTable(schemaName, tableName)) {
                String ddl = fromDataBase.getDDL(toType, schemaName, tableName, ColSizeTimes.EQUAL);
                System.out.println(ddl);
                toDataBase.createTable(schemaName, tableName, ddl);
            }
            System.out.println(tableName);
            List<String> columnNames = toDataBase.getTableColumns(schemaName, tableName);
            ResultSet rs = fromDataBase.readData(schemaName, tableName, columnNames, whereClause);
            toDataBase.writeData(schemaName, tableName, columnNames, rs, whereClause);
        } finally {
        }
    }
}

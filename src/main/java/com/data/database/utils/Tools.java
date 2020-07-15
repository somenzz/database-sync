package com.data.database.utils;
import java.io.File;
import java.io.Reader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class Tools {
    public static String buildInsertSql(String table, String[] fields) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(table).append(" (");
        sb.append(String.join(", ", fields));
        sb.append(") ");
        sb.append("VALUES (");
        for (int i = 0; i < fields.length; i++) {
            if (i == fields.length - 1) {
                sb.append("?").append(")");
                break;
            }
            sb.append("?").append(", ");
        }
        return sb.toString();
    }

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


}
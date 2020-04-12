package com.data.database;

import java.sql.*;
import com.data.database.api.DataSync;
import com.data.database.api.impl.*;
import java.util.List;

/**
 * Hello world!
 */
public final class App {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/kjt?useSSL=false&characterEncoding=utf8&serverTimezone=UTC";
    // 数据库的用户名与密码，需要根据自己的设置
    static final String USER = "aaron";
    static final String PASS = "aaron";

    private App() {
    }

    /**
     * Says hello to the world.
     * 
     * @param args The arguments of the program.
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        // 注册 JDBC 驱动
        Class.forName(JDBC_DRIVER);
        // 打开链接
        try (Connection conn = DriverManager.getConnection(DB_URL,USER,PASS)) {
            DataSync ds = new MySql();
            List<String> columns = ds.getTableColumns(conn, "user");
            for(String cl : columns){
                System.out.println(cl);
            }      
         
        }
    }
}

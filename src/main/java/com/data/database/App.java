package com.data.database;

import java.sql.*;
import com.data.database.api.DataSync;
import com.data.database.api.impl.*;
import java.util.List;

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
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        // 注册 JDBC 驱动
        // 打开链接
        String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
        String DB_URL = "jdbc:mysql://localhost:3306/aarondb?useSSL=false&characterEncoding=utf8&serverTimezone=UTC";
        String USER = "aaron";
        String PASS = "aaron";


        try {
            DataSync ds = new MySql(JDBC_DRIVER, DB_URL, USER, PASS);
            List<String> columns = ds.getTableColumns("iphone_contacts");
            for(String cl : columns){
                System.out.println(cl);
            }
            System.out.println(ds.getDDL("iphone_contacts"));
            ResultSet rs = ds.readData("iphone_contacts" , columns, null);

            DataSync ds2 = new MySql(JDBC_DRIVER, DB_URL, USER, PASS);
            ds2.writeData("iphone_contacts2", columns, rs, "1=1");

            // int numberOfcols = rs.getMetaData().getColumnCount();
            // while (rs.next()){
            //     for(int i=1; i <= numberOfcols;i++){
            //         System.out.print(rs.getObject(i));
            //         System.out.print('\t');
            //     }
            //     System.out.println();

            // }

        }finally{
        }
    }
}

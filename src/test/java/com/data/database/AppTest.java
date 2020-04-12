package com.data.database;

import org.junit.Test;

import org.junit.BeforeClass;
import org.junit.AfterClass;

import static org.junit.Assert.*;
import com.data.database.api.impl.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test.
     */

    private static Connection mysqlConn = null;

    @BeforeClass // 公开表态无返回值
    public static void beforeClass() throws Exception {
        // 每次测试类执行前执行一次，主要用来初使化公共资源等

        String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
        String DB_URL = "jdbc:mysql://localhost:3306/kjt?useSSL=false&characterEncoding=utf8&serverTimezone=UTC";
        // 数据库的用户名与密码，需要根据自己的设置
        String USER = "aaron";
        String PASS = "aaron";
        Class.forName(JDBC_DRIVER);
        mysqlConn = DriverManager.getConnection(DB_URL, USER, PASS);

    }

    @AfterClass // 公开表态无返回值
    public static void afterClass() throws Exception {
        // 每次测试类执行完成后执行一次，主要用来释放资源或清理工作
    }

    @Test
    public void testApp() {
        assertTrue(true);
    }

    @Test
    public void testMySql(){
        MySql mysql = new MySql();
        assertTrue(mysql.existsTable(mysqlConn, "user"));
        assertFalse(mysql.existsTable(mysqlConn, "user11"));

    }

}

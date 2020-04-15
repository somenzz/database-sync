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

    @BeforeClass // 公开表态无返回值
    public static void beforeClass() throws Exception {
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
        assertTrue(true);
    }

}

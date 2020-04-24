package com.data.database;

import org.junit.Test;

import org.junit.BeforeClass;
import org.junit.AfterClass;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
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
    public void testMySql() {
        System.out.println("===============");
        StringWriter ws = new StringWriter(100);
        ws.write("str1111111");
        System.out.println(ws.toString());
        System.out.println(ws.getBuffer().length());
        ws.getBuffer().setLength(4);
        System.out.println(ws.getBuffer().length());
        System.out.println(ws.toString());
        ws.write("str11222222211");
        System.out.println(ws.toString());
        System.out.println(String.format("%.2f", 12312312*1.0/1000));

        System.out.println("===============");

        assertTrue(true);
    }

}

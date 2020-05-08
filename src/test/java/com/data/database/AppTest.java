package com.data.database;

import org.junit.Test;
import com.data.database.api.DataSync;
import com.data.database.api.impl.DataBaseSync;
import com.data.database.api.MyEnum;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import static org.junit.Assert.*;

import java.io.StringWriter;
import java.sql.SQLException;
import java.sql.ResultSet;

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
    public void testMySql() throws SQLException, ClassNotFoundException {
        System.out.println("===============");
        // StringWriter sw = new StringWriter();
        int count = 10000;
        StringBuilder sBulider1 = new StringBuilder();
        StringBuilder sBulider2 = new StringBuilder();
        // StringBuffer sBuffer = new StringBuffer();
        double begin, end;
        // begin = System.currentTimeMillis();
        // for (int i =0;i< count;i++){
        // sw.write("10000"+i+'\n');
        // }
        // end=System.currentTimeMillis();
        // System.out.println("sw: "+(end-begin));
        // begin = System.currentTimeMillis();
        // for (int i =0;i< count;i++){
        // sBuffer.append("10000"+i+'\n');
        // }
        // end=System.currentTimeMillis();
        // System.out.println("sBuffer: "+(end-begin));

        begin = System.currentTimeMillis();
        for (int j = 0; j < 10000; j++) {
            for (int i = 0; i < count; i++) {
                sBulider2.append("10000" + i + '\n');
            }
            sBulider2.delete(0, sBulider2.length());

        }
        end = System.currentTimeMillis();
        System.out.println("sBulider2: delete " + (end - begin));

        begin = System.currentTimeMillis();
        for (int j = 0; j < 10000; j++) {
            for (int i = 0; i < count; i++) {
                sBulider1.append("10000" + i + '\n');
            }
            sBulider1.setLength(0);

        }
        end = System.currentTimeMillis();
        System.out.println("sBulider1: set length " + (end - begin));

        System.out.println("===============");
        // String type = "mysql";
        // String driver = "com.mysql.cj.jdbc.Driver";
        // String url =
        // "jdbc:mysql://localhost:3306/aarondb?useSSL=false&characterEncoding=utf8&serverTimezone=UTC";
        // String user = "aaron";
        // String password = "aaron";
        // DataBaseSync ds = new DataBaseSync(type,driver, url, user, password,null );
        // DataBaseSync ds2 = new DataBaseSync(type,driver, url, user, password,null );
        // ResultSet rsds1 = ds.getColMetaData("aarondb", "iphone_contacts");
        // ResultSet rsds2 = ds2.getColMetaData("aarondb", "iphone_contacts2");
        // App.compareColMetaData("mysql", "aarondb", "iphone_contacts2", rsds1, rsds2,
        // MyEnum.ColSizeTimes.EQUAL);
        assertTrue(true);
    }

}

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
import java.util.List;
import java.sql.ResultSet;

/**
 * Unit test for simple App.
 */
public class AppTest {
    /**
     * Rigorous Test.
     */

     @Test
     public void testApp(){
        App app = App.getInstance();
        assertTrue(true);
     }
}

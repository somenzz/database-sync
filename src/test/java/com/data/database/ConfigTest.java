package com.data.database;

import org.junit.Test;
import static org.junit.Assert.*;

public class ConfigTest{

    @Test
    public void testSingle(){
        Config config1 = Config.getInstance("mysql_test");
        Config config2 = Config.getInstance("mysql_test");
        assertTrue(config1 == config2);
    }

    @Test
    public void testOutput(){
        Config config1 = Config.getInstance("mysql_test");
        assertTrue(config1.getBufferRows() >= 1000);
    }

    
}
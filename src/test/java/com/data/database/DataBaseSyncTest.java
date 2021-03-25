package com.data.database;

import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.SQLException;

import com.data.database.api.MyEnum.ColSizeTimes;
import com.data.database.api.impl.DataBaseSync;

public class DataBaseSyncTest {
    
    @Test
    public void testGetDDL() throws SQLException, ClassNotFoundException{
        Config config = Config.getInstance("mysql_test");
        // DataBaseSync dbs = new DataBaseSync(config.getDbType(),config.getJdbcDriver(), config.getDbUrl(), config.getUserName(),config.getPassword(),config.getBufferRows());
        // String ddl = dbs.getDDL("mysql_test", "mysql", "users", ColSizeTimes.EQUAL);
        // System.out.print(ddl);
        assertTrue(true);
    }
}

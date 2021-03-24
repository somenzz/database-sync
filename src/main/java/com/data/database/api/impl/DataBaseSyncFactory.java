package com.data.database.api.impl;
import com.data.database.Config;
import java.util.Map;
import java.sql.SQLException;

public class DataBaseSyncFactory {
  
    public static DataBaseSync getInstance(Config config, boolean IsSpecificFeture) throws SQLException,ClassNotFoundException {
      DataBaseSync dataBaseSync  = null;
      if(!IsSpecificFeture){
          return new DataBaseSync(config.getDbType(), config.getJdbcDriver(),config.getDbUrl(),config.getUserName(),config.getPassword(),config.getBufferRows()); 
      }
      else if("postgres".equals(config.getDbType()) ){
          dataBaseSync = new PostgresDataBaseSync(config.getDbType(), config.getJdbcDriver(),config.getDbUrl(),config.getUserName(),config.getPassword(),config.getBufferRows()) ;
      }else{
          dataBaseSync = new DataBaseSync(config.getDbType(), config.getJdbcDriver(),config.getDbUrl(),config.getUserName(),config.getPassword(),config.getBufferRows()); 
      }
      return dataBaseSync; 
    }
}

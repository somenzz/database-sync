package com.data.database;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import com.data.database.utils.Tools;

public class Config{

    private String jdbcDriver;
    private String dbUrl;
    private String userName;
    private String password;
    private String dbType;
    private String encoding;
    private Integer bufferRows;
    private String tbspaceDDL;

    private static HashMap<String, Config> instanceMap = new HashMap<>();

    /*
    db_identifier 是 config.json 中的数据库的标识符，如样例配置文件中的 "mysql_test" 
    */
    private Config(String db_identifier){

        JSONObject jobj = JSON.parseObject(Tools.readJsonFile("./config/config.json"));
        this.jdbcDriver = jobj.getJSONObject(db_identifier).getString("driver");
        this.dbUrl = jobj.getJSONObject(db_identifier).getString("url");
        this.userName = jobj.getJSONObject(db_identifier).getString("user");
        this.password = jobj.getJSONObject(db_identifier).getString("password");
        this.dbType = jobj.getJSONObject(db_identifier).getString("type");
        this.encoding = jobj.getJSONObject(db_identifier).getString("encoding");
        this.tbspaceDDL = jobj.getJSONObject(db_identifier).getString("tbspace_ddl");
        this.bufferRows = (Integer) jobj.getOrDefault("buffer-rows", 100000);

    }


    //让配置类成为例类，每个数据库的配置信息在内存只保留一份

    public static synchronized Config getInstance(String db_identifier){
        Config instance;
        if(instanceMap.containsKey(db_identifier)){
            instance = instanceMap.get(db_identifier);
        }else{
            instance = new Config(db_identifier);
            instanceMap.put(db_identifier,instance);
        }
        return instance;
    }

    public String getJdbcDriver(){
        return this.jdbcDriver;
    }
    public String getDbUrl(){
        return this.dbUrl;
    }
    public String getUserName(){
        return this.userName;
    }

    public String getPassword(){
        return this.password;
    }

    public String getDbType(){
        return this.dbType;
    }

    public String getEncoding(){
        return this.encoding == null ? "utf-8":this.encoding;
    }

    public Integer getBufferRows(){
        return this.bufferRows;
    }

    public String getTbspaceDDL(){
        return this.tbspaceDDL == null? "": this.tbspaceDDL;
    }

}
# [database-sync](https://gitee.com/somenzz/table-sync)

#### 介绍

java 程序编写，真正跨平台。

传入一定的参数，即可在相同或不同的数据库间进行表的同步，包括表结构的同步及数据的同步。作业由调度工具进行调度，比如 moia，本项目旨在提供一种数据库间表同步的通用工具。

目前项目 demo 已经可以使用 ，欢迎感兴趣的朋友一起加入。

#### 程序的使用

数据库的信息写在配置文件中，计划支持各种主流关系型数据库，如 MysqL、Db2、Oracle、PostgreSQL。

程序名称叫 database-sync，运行方式是这样的：

#### 全量更新
```sh
java -jar database-sync.jar {fromDb} {fromSchema} {fromTable} {toDb} {toSchema} {toTable}
```
fromDb 是指配置在 config.json 的数据库信息，假如有以下配置文件：

```json
{
      "postgres":{
        "type":"postgres",
        "driver":"org.postgresql.Driver",
        "url":"jdbc:postgresql://localhost:5432/apidb",
        "user": "postgres",
        "password":"aaron"
    },


    "aarondb":{
        "type":"mysql",
        "driver":"com.mysql.cj.jdbc.Driver",
        "url":"jdbc:mysql://localhost:3306/aarondb?useSSL=false&characterEncoding=utf8&serverTimezone=UTC",
        "user": "aaron",
        "password":"aaron"
    }
}
```

则 fromDb、toDb 可以是 aarondb 或者 postgres。

- fromSchema 读取数据的表的模式名，可以填写 "".
- fromTable 读取数据的表明，必须提供。
- toSchema 写入数据表的模式名，可以填写 ""，可以和 fromSchema 不同.
- toTable 写入数据表的表名，必须提供，当写入表不存在时，自动按读取表的表结构创建，可以和 fromTable 不同。

数据在写入前会自动清理，每 100000 条记录读取一次，写入一次，对于上亿的数据量，也不会占有大内存。


#### 增量更新
```sh
java -jar database-sync.jar {fromDb} {fromSchema} {fromTable} {toDb} {toSchema} {toTable} [whereClause]
```
与全量更新的唯一区别是可以提供 where 条件，程序按 where 条件自动清理数据，写入数据。

#### 编写目的

提高数据库间表的同步效率，如果是轻加工，就丢掉低效的 datastage 和 kettle 吧。

#### 参与贡献

1. 张慧峰
2. 施云霄

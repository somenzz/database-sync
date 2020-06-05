# [database-sync](https://gitee.com/somenzz/table-sync)

#### 介绍

java 程序编写，真正跨平台。

简单的传入一定的参数，即可**跨数据库**实现以下功能：

1. 两个表之间数据的同步，可以增量或全量更新。
2. 两个表表结构的同步，包括自动建表，原表扩字段长度或增加字段，目标表也做相同动作。
3. 支持指定原表或目标表的字段序列，更灵活。默认按目标表的字段序列查询原表的字段序列。
4. 支持视图到表的数据抽取。
5. 日志记录、插入记录数统计、耗时统计。
结合调度工具，您可以轻松搭建一个数据仓库。

本程序的最大用处就是构建集市或数仓所需要的基础层数据源。

目前项目已经投入生产使用 ，欢迎感兴趣的朋友一起加入。

#### 程序的使用方法

数据库的信息写在配置文件中，计划支持各种主流关系型数据库，如 MysqL、Db2、Oracle、PostgreSQL。

程序名称叫 database-sync，运行方式是这样的：

```sh
(py37env) ➜  target git:(master) ✗ java -jar database-sync-1.1.jar
Usage:
java -jar database-sync-1.0.jar [options] {fromDB} {fromSchema} {fromTable} {toDB} {toSchema} {toTable} [whereClause]
options:
        --version or -v  :print version then exit
        --help or -h     :print help info then exit
        --simple or -s   :use insert into table A select * from B mode, ignore table's structure
        --from_fields={col1,col2} or -ff={col3,col4}   :specify from fields
        --to_fields={col1,col2} or -tf={col3,col4}   :specify to fields
```

帮助说明：

[] 中括号里的内容表示选填，例如 [options] 表示 options 下的参数不是必须的。

1、其中 options 参数解释如下：

`--simple` 或者 `-s` : 简单模式，此时只进行数据传输，不进行表构的同步。
`--from_fields=col1,col2` 或者 `-ff=col1,col2` : 指定原表的字段序列，注意 = 前后不能有空格。
`--to_fields=col3,col4` 或者 `-tf=col3,col4` : 指定目标表的字段序列，注意 = 前后不能有空格。

2、whereClause 表示 where 条件，用于增量更新，程序再插入数据前先按照 where 条件进行清理数据，然后按照 where 条件从原表进行读取数据。 whereClause 最好使用双引号包起来，表示一个完整的参数。如："jyrq='2020-12-31'"


{} 大括号里的内容表示必填。

`fromDb` 是指配置在 config.json 的数据库信息，假如有以下配置文件：

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

- `fromSchema` 读取数据的表的模式名，可以填写 "".
- `fromTable` 读取数据的表明，必须提供。
- `toSchema` 写入数据表的模式名，可以填写 ""，可以和 fromSchema 不同.
- `toTable` 写入数据表的表名，必须提供，当写入表不存在时，自动按读取表的表结构创建，可以和 fromTable 不同。


#### 增量更新
```sh
java -jar database-sync.jar {fromDb} {fromSchema} {fromTable} {toDb} {toSchema} {toTable} [whereClause]
```
与全量更新的唯一区别是可以提供 where 条件，程序按 where 条件自动清理数据，写入数据。


#### 文件配置

配置文件位于 config/config.json，如下所示：

```json

{
    "sjwb":{
        "type":"db2",
        "driver":"com.ibm.db2.jcc.DB2Driver",
        "url":"jdbc:db2://192.168.1.230:50000/wbsj",
        "user": "****",
        "password":"****",
        "encoding":"utf-8"
    },

    "dw_test":{
        "type":"db2",
        "driver":"com.ibm.db2.jcc.DB2Driver",
        "url":"jdbc:db2://192.168.169.99:60620/edwdb",
        "user": "****",
        "password":"****",
        "encoding":"gbk"
    },

    "postgres":{
        "type":"postgres",
        "driver":"org.postgresql.Driver",
        "url":"jdbc:postgresql://10.99.66.39:5432/apidb",
        "user": "****",
        "password":"****",
        "encoding":"utf-8"
    },


    "aarondb":{
        "type":"mysql",
        "driver":"com.mysql.cj.jdbc.Driver",
        "url":"jdbc:mysql://localhost:3306/aarondb?useSSL=false&characterEncoding=utf8&serverTimezone=UTC",
        "user": "****",
        "password":"****",
        "encoding":"utf-8"
    },

    "buffer-rows": 100000
}

```

配置文件说明：

`buffer-rows` 表示读取多少行时一块写入目标数据库，根据服务器内存大小自己做调整，100000 行提交一次满足大多数情况了。
`encoding` 用于表结构同步时，utf-8 库的字符串长度应该是 gbk 库字符串长度的 2 倍，可以解决字段含有中文的问题，为什么是 2 倍？ 为了字符串的长度不会出现小数位。


#### 编写目的

提高数据库间表的同步效率，如果是轻加工，就丢掉低效的 datastage 和 kettle 吧。

#### 参与贡献

1. 张慧峰
2. 施云霄
3. 高鹏
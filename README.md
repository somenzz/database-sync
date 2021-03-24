# [database-sync](https://gitee.com/somenzz/database-sync)

这是为数据开发人员使用的辅助工具，用于数据库之间的表同步，说同步并不严谨，因为不是实时更新的，更确切的说法是复制，可以方便的从一个数据库复制表到另一个数据库，以下遇到同步的词请理解为复制。

## 介绍

需求背景：

有很多业务系统，他们的数据库是相互独立的，需要把这些数据归集在一个数据库中（数据仓库），以便做数据统计和分析。希望能有这样的工具，输入数据库名，表名就可以将数据从源数据库拷贝到目标数据库中。具体需求如下：

- 能自动同步表结构，如：源表扩字段，目标表自动扩字段。
- 支持增量或全量同步数据，可以仅同步某个日期之后的数据。
- 支持指定字段同步，只同步关心的那些字段。
- 支持主流的关系型数据库: mysql、db2、postgresql、oracle、sqlserver
- 源表和目标表表名可以不同，字段名也可以不同（已存在目标表的情况下）

因为自己要用，我就自己写了一个，顺便熟悉下 java 开发（之前一直用 Python），本程序的最大用处就是构建集市或数仓所需要的基础层数据源，欢迎感兴趣的朋友一起加入。

## 程序的使用方法

### Docker 方式：

这里用到三个容器:
- app 也就是主程序本身，app 容器使用的程序文件就是 release 目录下的文件，已经做了绑定。
- mysql 测试用的数据库，已提前放好了 7000 条测试数据。
- postgres 测试用的数据库，没有数据。

先部署，执行 `docker-compose up -d` 就自动完成了部署：

```sh
$ git clone https://github.com/somenzz/database-sync.git
$ cd database-sync
$ docker-compose up -d
Creating database-sync_postgres_1 ... done
Creating database-sync_app_1      ... done
Creating database-sync_mysql_1    ... done
```
这样三个容器就启动了，使用 `docker ps -a |grep database-sync` 可以查看到三个正在运行的容器：

![](https://tva1.sinaimg.cn/large/008eGmZEgy1govcml0u7dj31xq07074x.jpg)

现在直接使用 `docker exec -i database-sync_app_1 java -jar database-sync-1.3.jar` 来执行程序：

![](https://tva1.sinaimg.cn/large/008eGmZEgy1govcoiltcnj31nc0b2js0.jpg)

mysql 容器已有测试数据，`release/config/config.json` 已经配置好了数据库的连接，因此可以直接试用，以下演示的是从 mysql 复制表和数据到 postgres：

1. 全量复制，自动建表：

```sh
docker exec -i database-sync_app_1 java -jar database-sync-1.3.jar mysql_test testdb somenzz_users postgres_test public users --sync-ddl
```
![](https://tva1.sinaimg.cn/large/008eGmZEgy1govcvnj1qij31bl0u0q8q.jpg)


如果你不想每次都敲 `docker exec -i database-sync_app_1` ，可以进入容器内部执行：

```sh
(py38env) ➜  database-sync git:(master) ✗ docker exec -it database-sync_app_1 /bin/bash
root@063b1dc76fe1:/app# ls
config	database-sync-1.3.jar  lib  logs
root@063b1dc76fe1:/app# java -jar database-sync-1.3.jar mysql_test testdb somenzz_users postgres_test public users -sd
```

2. 增量复制：

```sh
root@063b1dc76fe1:/app# java -jar database-sync-1.3.jar mysql_test testdb somenzz_users postgres_test public zz_users "create_at >= '2018-01-09'"
```
![](https://tva1.sinaimg.cn/large/008eGmZEgy1govpz9o5v2j327a0sygph.jpg)

3. 指定字段：

```sh
root@063b1dc76fe1:/app# java -jar database-sync-1.3.jar mysql_test testdb somenzz_users postgres_test public zz_users -ff="user_id,name,age" -tf="user_id,name,age" "create_at >= '2018-01-09'"
```
![](https://tva1.sinaimg.cn/large/008eGmZEgy1govq38gxfnj32go0t4jv7.jpg)



### 普通方式

程序运行前确保已安装 java 1.8 或后续版本，已经安装 maven，然后 clone 源码，打包：

```sh
git clone https://gitee.com/somenzz/database-sync.git
cd database-sync
mvn package
```
此时你会看到 target 目录，将 target 下的 lib 目录 和 database-sync-1.3.jar 复制出来，放在同一目录下，然后再创建一个 config 目录，在 config 下新建一个 config.json 文件写入配置信息，然后将这个目录压缩，就可以传到服务器运行了，请注意先充分测试，jdk 要求 1.8+

```sh
[aaron@hdp002 /home/aaron/App/Java/database-sync]$ ls -ltr
total 48
drwxr-xr-x 2 aaron aaron  4096 Apr 23  2020 lib
-rwxrw-r-- 1 aaron aaron   157 Jun 23  2020 run.sh
drwxrwxr-x 2 aaron aaron  4096 Jul  3  2020 logs
-rw-rw-r-- 1 aaron aaron 24773 Mar 16  2021 database-sync-1.3.jar
drwxr-xr-x 7 aaron aaron  4096 Aug  3  2020 jdk1.8.0_231
drwxrwxr-x 2 aaron aaron  4096 Feb 19 17:07 config
```

你也可以直接下载我打包好的使用。

程序名称叫 database-sync，运行方式是这样的：

```sh
(py38env) ➜  target git:(master) ✗ java -jar database-sync-1.3.jar -h      
Usage: 
java -jar database-sync-1.0.jar [options] {fromDB} {fromSchema} {fromTable} {toDB} {toSchema} {toTable} [whereClause]
options:
        --version or -v     :print version then exit
        --help or -h        :print help info then exit
        --sync-ddl or -sd   :auto synchronize table structure
        --from_fields=col1,col2 or -ff=col3,col4   :specify from fields
        --to_fields=col1,col2 or -tf=col3,col4   :specify to fields
        --no-feture or -nf  :will not use database's feture
```

帮助说明：

[] 中括号里的内容表示选填，例如 [options] 表示 options 下的参数不是必须的。

1、其中 options 参数解释如下：

`--sync-ddl` 或者 `-sd` : 加入该参数会自动同步表结构。
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
        "password":"aaron",
        "encoding": "utf-8"
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

fromDb、toDb 可以是 aarondb 或者 postgres。

- `fromSchema` 读取数据的表的模式名，可以填写 "".
- `fromTable` 读取数据的表明，必须提供。
- `toSchema` 写入数据表的模式名，可以填写 ""，可以和 fromSchema 不同.
- `toTable` 写入数据表的表名，必须提供，当写入表不存在时，自动按读取表的表结构创建，可以和 fromTable 不同。


## 增量更新
```sh
java -jar database-sync.jar {fromDb} {fromSchema} {fromTable} {toDb} {toSchema} {toTable} [whereClause]
```
与全量更新的唯一区别是可以提供 where 条件，程序先按 where 条件自动清理数据，再写入数据。


## 配置文件说明

配置文件位于 config/config.json，如下所示：

```json

{
    "sjwb":{
        "type":"db2",
        "driver":"com.ibm.db2.jcc.DB2Driver",
        "url":"jdbc:db2://192.168.1.*:50000/wbsj",
        "user": "****",
        "password":"****",
         "tbspace_ddl": "/*这里可以放置指定表空间的语句*/"
        "encoding":"utf-8"
    },

    "dw_test":{
        "type":"db2",
        "driver":"com.ibm.db2.jcc.DB2Driver",
        "url":"jdbc:db2://192.168.169.*:60990/dwdb",
        "user": "****",
        "password":"****",
        "encoding":"gbk"
    },

    "postgres":{
        "type":"postgres",
        "driver":"org.postgresql.Driver",
        "url":"jdbc:postgresql://10.99.**.**:5432/apidb",
        "user": "****",
        "password":"****",
        "tbspace_ddl": "WITH (compression=no, orientation=orc, version=0.12)\ntablespace hdfs\n",
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

`type`  表示数据库类型，均为小写：

- mysql
- postgres
- db2
- oracle
- sqlserver

`tbspace_ddl` 表示自动建表时指定的表空间，该选项不是必需的，可以删除。

`buffer-rows` 表示读取多少行时一块写入目标数据库，根据服务器内存大小自己做调整，100000 行提交一次满足大多数情况了。

`encoding` 用于表结构同步时确定字段长度，比如说源库的字段是 gbk varchar(10)，目标库是 utf-8，那么就应该为 varchar(15)，这样字段有中文就不会出现截断或插入失败问题，程序这里 2 倍，也就是 varchar(20) ，这样字段长度不会出现小数位。


## 编写目的

提高数据库间表的复制效率，如果不需要对源表字段进行转换，就丢掉低效的 datastage 和 kettle 吧。
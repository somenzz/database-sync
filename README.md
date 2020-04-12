# table-sync

#### 介绍
传入一定的参数，即可在相同或不同的数据库间进行表的同步，包括表结构的同步及数据的同步。作业的调试工具进行调度，本项目旨在提供一种数据库间表同步的通用工具。

目前项目正在开发中，欢迎感兴趣的朋友一起加入。

#### 软件架构

- 数据库的信息写在配置文件中，计划支持各种主流关系型数据库，如 MysqL、Db2、Oracle、PostgreSQL。
- 假如程序名称叫 table-sync，计划运行方式是这样的：

```shell

# 全量更新
java -jar table-sync.jar --from=mysqldb1.table1 --to=db2db1.table2 

# 按日期增量更新
java -jar table-sync.jar --from=mysqldb1.table1 --to=db2db1.table2 --where="data_date='YYYY-MM-DD'"

```

- 如果源数据库的表结构变化了，可以选择自动更新表结构，默认不自动更新

```sh
# 自动更新表结构
java -jar table-sync.jar --from=mysqldb1.table1 --to=db2db1.table2 --autoDDL=true

```





#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request


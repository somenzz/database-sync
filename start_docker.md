# docker 容器操作


## mysql 容器

启动 mysql 服务，数据保存在本地卷 database-sync，连接网络 database-sync，先创建网络 database-sync

```sh
docker run -d --name mysql \
--network database-sync \
--network-alias mysql \
-v database-sync:/var/lib/mysql \
-e MYSQL_ROOT_PASSWORD=root \
-e MYSQL_DATABASE=testdb \
mysql:latest
```

进入 mysql 容器内部环境，可查询数据

```sh
docker exec -it mysql env LANG=C.UTF-8 /bin/bash
```


## postgres 容器

启动 postgres 服务

```sh
docker run -d --name postgres \
--network database-sync \
--network-alias postgres \
-e  POSTGRES_PASSWORD=root \
postgres:latest
```

进入 posgres 容器内部：

```sh
docker exec -it postgres /bin/bash
```



## 启动 database-sync app 容器，连接到 database-sync 网络上

```sh
docker run -it --name app \
--network database-sync \
-v "$(pwd)/target:/app/release" \
java:latest \
/bin/bash
```

挂载 target 目录到 java 容器，jar 文件更新后，直接生效，方便 debug。 


eg: mysql 向 postgre 同步表结构及数据：

```sh
java -jar database-sync-1.3.jar mysql_test testdb somenzz_users postgres_test public users -sd -nf
```



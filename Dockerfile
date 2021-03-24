FROM mysql:latest

# 数据库名
ENV MYSQL_DATABASE testdb
 
# 默认根用户密码
ENV MYSQL_ROOT_PASSWORD root
 
# 拷贝初始化sql脚本
COPY ./create_insert_data.sql /docker-entrypoint-initdb.d/
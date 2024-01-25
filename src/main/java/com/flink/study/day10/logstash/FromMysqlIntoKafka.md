从数据库中国读取数据通过logstash工具写入到kafka
# 下载logstash后，进入到该目录
cd /home/logstash-7.9.2

# 编写logstash的配置文件
创建logstash的配置文件

配置文件
```text
input {
    jdbc {
        jdbc_connection_url => "jdbc:mysql://192.168.222.132:3306/test_db?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=UTC"
        jdbc_user => "root"
        jdbc_password => "123456"
        # mysql-connector-java-5.1.42.jar
        jdbc_driver_library_path => "/home/mysql-connector-java-8.0.19.jar"  
        jdbc_driver_class => "com.mysql.cj.jdbc.Driver"
        statement => "select * from test_db.test_table where id>sql_last_id"
        use_column_values => true
        tracking_column => "id"
        tracking_column_type => "numeric" #这里只能写numeric或timestamp
        record_last_run => true
        scheduler => "cron表达式" #定时任务
    }
}

output {
    kafka {
        topic_id => "test-topic"
        bootstrap_servers => "192.168.222.132:9092"
        codec => plain {format => "%{message}"}
    }
}
```
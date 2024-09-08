从mysql数据库中国读取数据通过logstash工具写入到kafka
# 启动kafka
```shell
1.先启动zookeeper服务
/home/kafka_2.12-2.7.0/bin/zookeeper-server-start.sh $KAFKA/config/zookeeper.properties >> zookeeper.log &
cat /home/kafka_2.12-2.7.0/zookeeper.log 查看zk日志

2.再启动kafka服务
/home/kafka_2.12-2.7.0/bin/kafka-server-start.sh /home/kafka_2.12-2.7.0/config/server.properties >> kafka.log &
3.检查启动是否成功  jps
QuorumPeerMain	zookeeper服务启动成功的标志
Kafka			kafka服务启动成功的标志

4.新建一个主题
sh /home/kafka_2.12-2.7.0/bin/kafka-topics.sh  --create  --topic  order-topic  --partitions 1  --replication-factor 1  --bootstrap-server  192.168.222.132:9092
5.检查自己是否有某个主题
sh /home/kafka_2.12-2.7.0/bin/kafka-topics.sh  --bootstrap-server  192.168.222.132:9092  --describe  --topic  order-topic
6.启动一个生产者，这里是用kafka把自己当成生产者 我们演示案例这里不用
sh /home/kafka_2.12-2.7.0/bin/kafka-console-producer.sh  --broker-list  192.168.222.132:9092  --topic  order-topic
7.启动一个消费者窗口
sh /home/kafka_2.12-2.7.0/bin/kafka-console-consumer.sh  --bootstrap-server  192.168.222.132:9092  --topic  order-topic
```

# 下载logstash后，进入到该目录
cd /home/logstash-7.9.2/humm

# 编写logstash的配置文件
查看windows的宿主机ip 192.168.2.74
创建logstash的配置文件，放到 t2.conf
配置文件
```text
input{
	jdbc{
        jdbc_connection_string => "jdbc:mysql://192.168.2.74:3306/ecshop?autoReconnect=true"
        jdbc_user => "root"
        jdbc_password => "123456"
        jdbc_driver_library => "/home/mysql-connector-java-5.1.42.jar"
        jdbc_driver_class => "com.mysql.jdbc.Driver"
        statement => "select mem_total_nums,mem_order_nums,order_nums, mem_order_nums/mem_total_nums buy_rate, now() from (select count(1) mem_total_nums from ecs_users) a,(select count(1) order_nums from ecs_order_info) b,(select count(distinct a.user_id) mem_order_nums from ecs_users a join ecs_order_info b on a.user_id = b.user_id) c"
        schedule => "* * * * *"
  }
}

filter{
	mutate{
        remove_field => ["@timestamp"]
        remove_field => ["@version"]
        remove_field => ["host"]
  }
}

output{
	kafka{
        bootstrap_servers => "192.168.222.132:9092"
        topic_id => "order-topic"
        codec => json_lines
  }
}
```

# 启动logstash
```shell
/home/logstash-7.9.2/bin/logstash -f  /home/logstash-7.9.2/humm/t2.conf
```

# 观察kafka是否在接收数据


将文件作为数据源通过logstash工具写入到kafka
# 下载logstash后，进入到该目录
cd /home/logstash-7.9.2

# 编写logstash的配置文件
读取日志文件，先创建日志文件，然后创建logstash的配置文件

配置文件
也可以先配这个试下，意思是在屏幕上输入什么就作为数据源输出什么
```shell
input { stdin { } }

filter {
  grok {
    match => { "message" => "%{COMBINEDAPACHELOG}" }
  }
  date {
    match => [ "timestamp" , "dd/MMM/yyyy:HH:mm:ss Z" ]
  }
}
 
output {
  stdout { codec => rubydebug }
}
```

```text
input {
    file {
        path => "/home/logstash-7.9.2/humm/t1.log"
        start_position => "beginning"
    }
}

output {
    kafka {
        topic_id => "test-topic"
        bootstrap_servers => "192.168.222.132:9092"
    }
}

filter {
    # You can add filters here if needed、
    mutate {
        remove_field => ["@timestamp"]
        remove_field => ["host"]
    }
}

# 删除host字段时，不会删除master所占占位符，在output加入
output {
    kafka {
        topic_id => "test-topic"
        bootstrap_servers => "192.168.222.132:9092"
        codec => plain {format => "%{message}"}
    }
}
```
创建好后，启动zk+kafka
```shell
# 启动zk
/home/kafka_2.12-2.7.0 等价于 $KAFKA
$KAFKA/bin/zookeeper-server-start.sh $KAFKA/config/zookeeper.properties >> zookeeper.log &
cat /home/kafka_2.12-2.7.0/zookeeper.log 查看zk日志

# 启动kafka
$KAFKA/bin/kafka-server-start.sh $KAFKA/config/server.properties >> kafka.log &
cat /home/kafka_2.12-2.7.0/kafka.log 查看zk日志

# 新建一个主题
sh $KAFKA/bin/kafka-topics.sh  --create  --topic  test-topic  --partitions 1  --replication-factor 1  --bootstrap-server  192.168.222.132:9092

7.启动一个消费者窗口
sh /home/kafka_2.12-2.7.0/bin/kafka-console-consumer.sh  --bootstrap-server  192.168.222.132:9092  --topic  test-topic
```

# 启动kafka完成后，启动logstash
```shell
/home/logstash-7.9.2/bin/logstash -f /home/logstash-7.9.2/humm/t1.conf
```

# 写入数据，后查看kafka消费者会打印对应写入数据
```shell
echo aa,bb,cc >> /home/logstash-7.9.2/humm/t1.log
echo dd,ff,gg >> /home/logstash-7.9.2/humm/t1.log
```

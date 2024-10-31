
## kakfka 安装过程
```
# 需要安装jdk
 apt-get install openjdk-11-jdk

# https://kafka.apache.org/quickstart 
wget https://downloads.apache.org/kafka/3.8.0/kafka-3.8.0-src.tgz
tar -xzf kafka_2.13-3.8.0.tgz
cd kafka_2.13-3.8.0

./gradlew jar -PscalaVersion=2.13.14


# 安装到此完成，测试能否运行：
 KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
 # Format Log Directories
 bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
 # Start the Kafka Server
 bin/zookeeper-server-start.sh config/zookeeper.properties
 
 # 另起一个终端
 bin/kafka-server-start.sh config/server.properties

```

# Python 
```
pip install git+https://github.com/dpkp/kafka-python.git
```


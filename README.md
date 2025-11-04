# mqtt
Mqtt
基于Netty实现的 MqttClient,MqttServer,目前没有对用户名/密码进行认证; 

会将消息持久化到磁盘, Qos=0时，会直接响应确认，Qos=1 或者Qos=2时会确保消息写入pagecache再返回

## 使用
### 服务端启动

```java
//启动一个端口号为 9999 的 mqtt 服务端
public class TestMqttServer {
    public  static void main(String[] args) {
        new MqttServer(9999).start();
    }
}
```
### 消息发送方启动
```java
public class TestPublish {
    public static void main(String[] args) throws InterruptedException {
        //创建一个客户端，连接到服务端
        MqttClient mqttClient = new MqttClient(9999,"localhost");
        //指定连接信息
        MqttConnectOptions options = new MqttConnectOptions();
        options.setClientIdentifier("mqttPublisher");
        options.setUserName("aa");
        options.setPassword("bb".getBytes());
        Publisher publisher = mqttClient.connect(options);
        //消息主题是 hello
        Message message = new Message("hello",new byte[1024],2);
        // ack 后才 发送下一条
        publisher.publish(message).waitForAck();
        publisher.publish(message).waitForAck();
    }
}
```

### 消息订阅方启动
```java
public class TestSubscribe {
    public static void main(String[] args) {
        //创建到服务端的连接
        MqttClient mqttClient = new MqttClient(9999, "localhost");
        //指定连接信息
        MqttConnectOptions options = new MqttConnectOptions();
        options.setClientIdentifier("mqttSubscribe");
        options.setUserName("aa");
        options.setPassword("bb".getBytes());
        //创建 Publisher，用于发送一个订阅
        Publisher publisher = mqttClient.connect(options);
        // 订阅 hello， 并指定 Qos，以及接收到消息时的回调函数
        publisher.sendSubscribe("hello", MqttQoS.EXACTLY_ONCE.value(), System.out::println);
    }
}

```

### 使用python连接 mqtt

python需要安装paho-mqtt,项目启动后，可以通过如下脚本连接测试

```python

import paho.mqtt.client as mqtt

# MQTT Broker 信息
broker = "127.0.0.1"   # 
port = 9999
topic = "hello"

# 当连接成功时回调
def on_connect(client, rc):
    if rc == 0:
        print("✅ 连接成功")
        # 订阅主题
        client.subscribe(topic)
    else:
        print("❌ 连接失败，返回码：", rc)

# 当收到消息时回调
def on_message(msg):
    print(f"📩 收到消息: {msg.topic} -> {msg.payload.decode()}")

# 创建 MQTT 客户端
client = mqtt.Client(client_id="python_mqtt_demo")

# 设置回调函数
client.on_connect = on_connect
client.on_message = on_message

# 连接到 Broker
client.connect(broker, port, keepalive=60)

# 发布一条消息
client.publish(topic, "Hello MQTT from Python!")

# 循环等待（保持连接)
client.loop_forever()


```
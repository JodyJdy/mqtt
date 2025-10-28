# mqtt
Mqtt
基于Netty实现的 MqttClient,MqttServer,目前只实现Qos 0

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
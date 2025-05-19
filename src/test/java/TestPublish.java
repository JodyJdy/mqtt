import mqtt.mqttclient.MqttClient;
import mqtt.mqttclient.MqttConnectOptions;
import mqtt.mqttclient.Publisher;
import mqtt.storage.Message;

/**
 *测试类
 **/

public class TestPublish {
    public static void main(String[] args) {
        MqttClient mqttClient = new MqttClient(9999,"localhost");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setClientIdentifier("mqttPublisher");
        options.setUserName("aa");
        options.setPassword("bb".getBytes());
        Publisher publisher = mqttClient.connect(options);
        Message message = new Message("hello",new byte[1024],2);
        // ack 后才 发送下一条
        publisher.publish(message).waitForAck();
        publisher.publish(message).waitForAck();

    }
}

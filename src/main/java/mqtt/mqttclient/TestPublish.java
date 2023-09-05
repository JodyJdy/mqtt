

package mqtt.mqttclient;

import io.netty.util.CharsetUtil;
import mqtt.storage.Message;

import java.util.ArrayList;
import java.util.List;

/**
 *测试类
 **/

public class TestPublish {
    public static void main(String[] args) throws InterruptedException {
        MqttClient mqttClient = new MqttClient(9999,"localhost");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setClientIdentifier("mqttPublisher");
        options.setUserName("aa");
        options.setPassword("bb".getBytes());
        Publisher publisher = mqttClient.connect(options);
        Message message = new Message("hello",new byte[1024],2);
        Thread.sleep(3000);
        long start = System.currentTimeMillis();
        // ack 后才 发送下一条
        for(int i=0;i<3000000;i++){
            publisher.publish(message).waitForAck();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);

    }
}

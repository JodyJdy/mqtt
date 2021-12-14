

package mqtt.mqttclient;

import io.netty.util.CharsetUtil;
import mqtt.storage.Message;

import java.util.ArrayList;
import java.util.List;

/**
 *测试类
 **/

public class Tester {
    public static void main(String[] args) throws InterruptedException {
        MqttClient mqttClient = new MqttClient(9999,"localhost");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setClientIdentifier("mqtt");
        options.setUserName("aa");
        options.setPassword("bb".getBytes());
        Publisher publisher = mqttClient.connect(options);
        Message message = new Message("hello","hello world".getBytes(CharsetUtil.UTF_8),1);
        List<PublishResult> publishResults = new ArrayList<>();

        Thread.sleep(10000);
        long start = System.currentTimeMillis();
        for(int i=0;i<50000;i++){
            publishResults.add(publisher.publish(message));
        }
        publishResults.forEach(PublishResult::waitForAck);
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}

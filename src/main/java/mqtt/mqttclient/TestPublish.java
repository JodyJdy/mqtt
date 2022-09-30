

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
        Message message = new Message("hello","hello world".getBytes(CharsetUtil.UTF_8),1);
        List<PublishResult> publishResults = new ArrayList<>();
        Thread.sleep(1000);

        long start = System.currentTimeMillis();
        for(int i=0;i<3;i++){
            publishResults.add(publisher.publish(message));
        }
        publishResults.get(0).waitForAck();
        publishResults.forEach(PublishResult::waitForAck);
        long end = System.currentTimeMillis();
        System.out.println(end - start);

    }
}

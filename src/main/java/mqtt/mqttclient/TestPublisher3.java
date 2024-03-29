package mqtt.mqttclient;

import io.netty.util.CharsetUtil;
import mqtt.storage.Message;

import java.util.ArrayList;
import java.util.List;

public class TestPublisher3 {
    public static void main(String[] args) throws InterruptedException {
        MqttClient mqttClient = new MqttClient(9999,"localhost");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setClientIdentifier("mqttPublisher");
        options.setUserName("aa");
        options.setPassword("bb".getBytes());
        Publisher publisher = mqttClient.connect(options);
        Message message = new Message("hello",new byte[50 * 1024],1);
        List<PublishResult> publishResults = new ArrayList<>();
        Thread.sleep(3000);

        long start = System.currentTimeMillis();
        for(int i=0;i<3000000;i++){
            publishResults.add(publisher.publish(message));
        }
        publishResults.forEach(PublishResult::waitForAck);
        long end = System.currentTimeMillis();
        System.out.println(end - start);

    }
}

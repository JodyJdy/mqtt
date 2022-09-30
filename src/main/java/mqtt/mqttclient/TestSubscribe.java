package mqtt.mqttclient;

import mqtt.enums.MqttQoS;

import java.util.concurrent.atomic.AtomicInteger;

public class TestSubscribe {
    public static void main(String[] args) throws InterruptedException {
        MqttClient mqttClient = new MqttClient(9999,"localhost");

        MqttConnectOptions options = new MqttConnectOptions();
        options.setClientIdentifier("mqttSubscribe");
        options.setUserName("aa");
        options.setPassword("bb".getBytes());
        Publisher publisher = mqttClient.connect(options);
        publisher.sendSubscribe("hello", MqttQoS.EXACTLY_ONCE.value(), x->{
            System.out.println("收到消息:" + x);
        });

        Thread.sleep(10000);
        System.out.println("取消订阅");

        publisher.sendUnsubscribe("hello");
    }
}

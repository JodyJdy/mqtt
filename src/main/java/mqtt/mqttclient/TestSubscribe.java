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
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        long start = System.currentTimeMillis();

        publisher.sendSubscribe("hello", MqttQoS.EXACTLY_ONCE.value(), x->{
//            int l = atomicInteger.getAndIncrement();
//            if(l % 100000 ==0){
//                System.out.println(System.currentTimeMillis() - start);
//            }
            System.out.println(x);
        });
        Thread.sleep(5*1000);
        publisher.sendUnsubscribe("hello");
    }
}

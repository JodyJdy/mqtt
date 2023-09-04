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
//                double seconds = (System.currentTimeMillis() - start) / 1000.0;
//                if (seconds == 0) {
//                    seconds = 1;
//                }
//                System.out.println(l / seconds);
//            }
            System.out.println(x);
        });
    }
}

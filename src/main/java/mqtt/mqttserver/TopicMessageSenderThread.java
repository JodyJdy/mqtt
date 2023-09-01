package mqtt.mqttserver;

import io.netty.channel.Channel;
import mqtt.protocol.MqttMessage;
import mqtt.storage.Message;
import mqtt.storage.MessageStorage;
import mqtt.util.MqttMessageUtil;

import java.util.Set;

public class TopicMessageSenderThread extends Thread{
    private final MessageStorage messageStorage;
    private final UserSessions userSessions;
    private final String topic;
    private volatile boolean stop = false;

    public TopicMessageSenderThread(MessageStorage messageStorage, UserSessions userSessions, String topic) {
        this.messageStorage = messageStorage;
        this.userSessions = userSessions;
        this.topic = topic;
    }

    @Override
    public void run() {
        System.out.println("任务启动");
        while (!stop) {
            final Message message = messageStorage.readMessage(topic);
            if (message == null) {
                continue;
            }
            Set<Receiver> receiverSet = userSessions.getReceiver(message.getTopic());
            receiverSet.forEach(receiver -> {
                Channel channel = userSessions.getUser(receiver.getId()).getChannel();
                MqttMessage msg = MqttMessageUtil.publish(message, receiver.getMqttQoS());
                channel.writeAndFlush(msg);
            });
        }
        System.out.println("任务结束");
    }
    public void stopSend(){
        stop = true;
    }
}

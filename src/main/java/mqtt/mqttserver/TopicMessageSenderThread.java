package mqtt.mqttserver;

import io.netty.channel.Channel;
import mqtt.enums.MqttQoS;
import mqtt.protocol.MqttMessage;
import mqtt.storage.Message;
import mqtt.storage.MessageStorage;
import mqtt.util.MqttMessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class TopicMessageSenderThread extends Thread{
    public   static final Logger logger = LoggerFactory.getLogger(TopicMessageSenderThread.class);

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
        logger.info("topic:{} 消息发送任务启动", topic);
        while (!stop) {
            final Message message = messageStorage.readMessage(topic);
            if (message == null) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                   break;
                }
                continue;
            }
            Set<Receiver> receiverSet = userSessions.getReceiver(message.getTopic());
            receiverSet.forEach(receiver -> {
                Channel channel = userSessions.getUser(receiver.getId()).getChannel();
                //qos采用 Min(receiver.qos, message.qos)
                int qos = Math.min(receiver.getMqttQoS().value(), message.getQos());
                MqttMessage msg = MqttMessageUtil.publish(message, MqttQoS.valueOf(qos));
                channel.writeAndFlush(msg);
            });
        }
        logger.info("topic:{} 消息发送任务结束", topic);
    }
    public void stopSend(){
        stop = true;
    }
}



package mqtt.storage;

import io.netty.channel.Channel;
import mqtt.mqttserver.Receiver;
import mqtt.mqttserver.UserSessions;
import mqtt.protocol.MqttMessage;
import mqtt.util.MqttMessageUtil;

import java.util.Set;

/**
 *从中转队列里面读取消息进行转发
 *
 */

public class MessageQueueReader extends Thread {
    private final  MessageQueue messageQueue;
    private final UserSessions userSessions;

    public MessageQueueReader(MessageQueue messageQueue,UserSessions userSessions) {
        this.messageQueue = messageQueue;
        this.userSessions = userSessions;
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            Message message = messageQueue.getMessage();
            Set<Receiver> receiverSet = userSessions.getReciever(message.getTopic());
            if(receiverSet.isEmpty()){
                continue;
            }
            receiverSet.forEach(receiver -> {
                Channel channel = userSessions.getUser(receiver.getId()).getChannel();
                MqttMessage msg = MqttMessageUtil.publish(message,receiver.getMqttQoS());
                channel.writeAndFlush(msg);
            });
        }
    }
}

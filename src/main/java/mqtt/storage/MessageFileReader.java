

package mqtt.storage;

/**
 *
 * @date 2021/12/13
 **/

import io.netty.channel.Channel;
import mqtt.mqttserver.Receiver;
import mqtt.mqttserver.UserSessions;
import mqtt.protocol.MqttMessage;
import mqtt.util.MqttMessageUtil;

import java.io.IOException;
import java.util.Set;

/**
 * 从文件里面读取消息
 */
public class MessageFileReader extends Thread {
    private final FileProcess fileProcess;
    private final UserSessions userSessions;

    public MessageFileReader(FileProcess fileProcess, UserSessions userSessions) {
        this.fileProcess = fileProcess;
        this.userSessions = userSessions;
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            try {
                final Message  message = fileProcess.readMessage();
                if(message == null){
                    continue;
                }
                Set<Receiver> receiverSet = userSessions.getReciever(message.getTopic());
                if(receiverSet.isEmpty()){
                    continue;
                }
                receiverSet.forEach(receiver -> {
                    Channel channel = userSessions.getUser(receiver.getId()).getChannel();
                    MqttMessage msg = MqttMessageUtil.publish(message,receiver.getMqttQoS());
                    channel.writeAndFlush(msg);
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

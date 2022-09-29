

package mqtt.storage;

import io.netty.channel.Channel;
import mqtt.mqttserver.Receiver;
import mqtt.mqttserver.UserSessions;
import mqtt.protocol.MqttMessage;
import mqtt.util.MqttMessageUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 从消息文件里面读取消息
 */
public class MessageFileReader extends Thread {
    private final MessageStorage messageStorage;
    private final UserSessions userSessions;

    private long lastUpdateUsingTopics = -1;
    /**
     * 当前在使用中的topic, 1s更新一次
     */
    private List<String> usingTopics;

    public MessageFileReader(MessageStorage messageStorage, UserSessions userSessions) {
        this.messageStorage = messageStorage;
        this.userSessions = userSessions;
    }
    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            try {
                //如果当前没有在使用的topic，说明没有消息写入
                updateIfNecessary();
                if(usingTopics == null || usingTopics.isEmpty()){
                    Thread.sleep(500);
                }
                boolean hasMsg = false;
                for(String topic : usingTopics){
                    final Message  message = messageStorage.readMessage(topic);
                    if(message == null){
                        continue;
                    }
                    hasMsg = true;
                    Set<Receiver> receiverSet = userSessions.getReceiver(message.getTopic());
                    receiverSet.forEach(receiver -> {
                        Channel channel = userSessions.getUser(receiver.getId()).getChannel();
                        MqttMessage msg = MqttMessageUtil.publish(message,receiver.getMqttQoS());
                        channel.writeAndFlush(msg);
                    });
                }
                if(!hasMsg){
                    Thread.sleep(500);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    private void updateIfNecessary(){
       if(System.currentTimeMillis() - lastUpdateUsingTopics > 1000L){
           usingTopics = new ArrayList<>();
           for(String topic : messageStorage.topicSet()){
               if(!userSessions.getReceiver(topic).isEmpty()){
                   usingTopics.add(topic);
               }
           }
           lastUpdateUsingTopics = System.currentTimeMillis();
       }
    }
}

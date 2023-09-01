package mqtt.mqttserver;

import mqtt.storage.MessageStorage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 主题消息发送器
 */
public class TopicMessageSender {

    private final MessageStorage messageStorage;
    private final UserSessions userSessions;
    private final Map<String,TopicMessageSenderThread> task = new ConcurrentHashMap<>();

    public TopicMessageSender(MessageStorage messageStorage, UserSessions userSessions) {
        this.messageStorage = messageStorage;
        this.userSessions = userSessions;
    }

    public void addTask(String topic){
        TopicMessageSenderThread thread = new TopicMessageSenderThread(messageStorage, userSessions, topic);
        task.put(topic, thread);
        thread.start();
    }
    public void removeTask(String topic){
        //不保证立即停止
        messageStorage.stopRead(topic);
        task.remove(topic).stopSend();
    }
}

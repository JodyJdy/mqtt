

package mqtt.mqttserver;

import mqtt.enums.MqttQoS;
import mqtt.protocol.payload.MqttConnectPayload;
import mqtt.protocol.MqttTopic;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * 服务端存储所有用户的会话信息
 **/

public class UserSessions {
    /**
     * 用户名 -> Session之间的映射
     */
    private final Map<String, Session> idToUser = new HashMap<>(2);
    /**
     *  topic 到订阅用户之间的缓存,避免每次都进行查询
     */
    private Map<String,Set<Receiver>> topicSubscriberMap = new ConcurrentHashMap<>();


    private TopicMessageSender topicMessageSender;


    public void setTopicMessageSender(TopicMessageSender topicMessageSender) {
        this.topicMessageSender = topicMessageSender;
    }

    public Session getUser(String id){
        return idToUser.get(id);
    }
    /**
     * 添加一个用户
     */
    public void addUser(Session session){
        MqttConnectPayload payload = session.getMqttConnectPayload();
        idToUser.put(payload.getClientIdentifier(), session);
    }
    /**
     * 删除一个用户
     */
    public synchronized void rmUser(String id){
        idToUser.remove(id);
    }
    /**
     * 添加订阅
     */
    public synchronized void addSub(String id, List<MqttTopic> subs){
        Session session = idToUser.get(id);
        session.addSubscribe(subs);
        List<String> newTask = new ArrayList<>();
        for (MqttTopic mqttTopic : subs) {
            String topic = mqttTopic.getTopic();
            MqttQoS qos = session.getQos(topic);
            Receiver receiver = new Receiver(id,qos);
            //进行初始化
            if (!topicSubscriberMap.containsKey(topic)) {
                //建立相关任务
                newTask.add(topic);
                topicSubscriberMap.put(topic,Collections.synchronizedSet(new HashSet<>()));
            }
            topicSubscriberMap.get(topic).add(receiver);
        }
        newTask.forEach(topicMessageSender::addTask);
    }
    /**
     * 删除订阅
     */
    public synchronized void rmSub(String id, List<String> subs){
        Session session = idToUser.get(id);
        session.rmSubscribe(subs);
        for(String  topic : subs) {
            //删除订阅
            if (topicSubscriberMap.containsKey(topic)) {
                Set<Receiver> set = topicSubscriberMap.get(topic);
                set.removeIf(r -> Objects.equals(r.getId(), id));
                //清空
                if (set.isEmpty()) {
                    topicSubscriberMap.remove(topic);
                    //关闭相关任务
                    topicMessageSender.removeTask(topic);
                }
            }
        }
    }
    /**
     * 根据发布的topic 获取对应订阅用户
     */
    public  Set<Receiver> getReceiver(String topic){
        if(topicSubscriberMap.containsKey(topic)){
            return topicSubscriberMap.get(topic);
        }
        return new HashSet<>();
    }

}

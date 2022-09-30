

package mqtt.mqttserver;

import mqtt.enums.MqttQoS;
import mqtt.protocol.payload.MqttConnectPayload;
import mqtt.protocol.MqttTopic;
import mqtt.util.TopicUtil;

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
    private final Map<String, Session> idToUser = new ConcurrentHashMap<>(2);
    /**
     * topic 到订阅用户之间的缓存,避免每次都进行查询
     */
    private final Map<String,Set<Receiver>> topic2User = new ConcurrentHashMap<>(2);
    /**
     * 活跃的Topic
      */
    private final Set<String> topics = Collections.synchronizedSet(new HashSet<>());



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
     * 删除所有订阅
     */
    public synchronized void rmUser(String id){
        idToUser.remove(id);
        final Set<String> emptyReceiverTopic = new HashSet<>();
        topic2User.forEach((k,v)->{
            v.removeIf(r -> id.equals(r.getId()));
            if(v.isEmpty()){
                emptyReceiverTopic.add(k);
            }
        });
        //删除无接收者的topic
        emptyReceiverTopic.forEach(topic2User::remove);
    }
    /**
     * 添加订阅
     */
    public synchronized void addSub(String id, List<MqttTopic> subs){
        Session session = idToUser.get(id);
        session.addSubscribe(subs);
        for(MqttTopic sub : subs){
           String subscribe = sub.getTopic();
            topics.forEach(topic->{
                if(TopicUtil.isMatch(subscribe,topic)){
                    if(!topic2User.containsKey(topic)) {
                        topic2User.put(topic, new HashSet<>());
                    }
                    topic2User.get(topic).add(new Receiver(id,sub.getQoS()));
                }
            });
        }
    }
    /**
     * 删除订阅
     */
    public synchronized void rmSub(String id, List<String> subs){
        Session session = idToUser.get(id);
        session.rmSubscribe(subs);
        for(String subscribe : subs){
            topics.forEach(topic->{
                if(TopicUtil.isMatch(subscribe,topic)){
                    if(topic2User.containsKey(topic)) {
                        Set<Receiver> receivers = topic2User.get(topic);
                        receivers.removeIf(r->r.getId().equals(id));
                        //如果以及是空的，就删除 topic 到接受者的映射
                        if(receivers.isEmpty()){
                            topic2User.remove(topic);
                        }
                    }
                }
            });
        }

    }
    public Set<String> getUsingTopics(){
        return topics;
    }
    /**
     * 添加使用中的topic
     */
    public void addUsingTopic(String topic){
        topics.add(topic);
        Set<Receiver> receivers = new HashSet<>();
        idToUser.forEach((id,session)->{
            session.getSubscribe().forEach(
                    s->{
                        MqttQoS qos = session.match(topic);
                        if(qos != null){
                            receivers.add(new Receiver(id,qos));
                        }
                    }
            );
        });
        topic2User.put(topic,receivers);
    }
    /**
     * 根据topic 获取对应订阅用户
     */
    public  Set<Receiver> getReceiver(String topic){
        return topic2User.get(topic);
    }
    public synchronized void deleteUsingTopics(Set<String> t){
         topics.removeAll(t);
         t.forEach(topic2User::remove);
    }

}

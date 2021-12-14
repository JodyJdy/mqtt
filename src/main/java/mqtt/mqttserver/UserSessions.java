

package mqtt.mqttserver;

import mqtt.enums.MqttQoS;
import mqtt.protocol.payload.MqttConnectPayload;
import mqtt.protocol.MqttTopic;

import java.util.*;

/**
 *
 * 服务端存储所有用户的会话信息
 **/

public class UserSessions {
    /**
     * 用户名 -> Session之间的映射
     */
    private Map<String, Session> idToUser = new HashMap<>(2);
    /**
     * 发布 到订阅用户之间的缓存,避免每次都进行查询
     */
    private Map<String,Set<Receiver>> publishToUser = new HashMap<>(2);


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
        //重置缓存
        publishToUser = new HashMap<>(2);
    }
    /**
     * 添加订阅
     */
    public synchronized void addSub(String id, List<MqttTopic> subs){
        Session session = idToUser.get(id);
        session.addSubSscribe(subs);
        //重置缓存
        publishToUser = new HashMap<>(2);
    }
    /**
     * 删除订阅
     */
    public synchronized void rmSub(String id, List<String> subs){
        Session session = idToUser.get(id);
        session.rmSubscribe(subs);
        //重置缓存
        publishToUser = new HashMap<>(2);

    }
    /**
     * 根据发布的topic 获取对应订阅用户
     */
    public  Set<Receiver> getReciever(String publish){
        if(publishToUser.containsKey(publish)){
            return publishToUser.get(publish);
        }
        Set<Receiver> result = new HashSet<>();
        idToUser.forEach((key,value)->{
            MqttQoS qos = value.isMatch(publish);
            if(qos != null){
                result.add(new Receiver(key,qos));
            }
        });
        publishToUser.put(publish,result);
        return result;
    }

}

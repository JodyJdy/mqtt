
package mqtt.mqttclient;

import mqtt.storage.Message;

/**
 * 消息监听器，消息进来时处理消息
 */
public interface MessageListener {
    void receiveMsg(Message msg);
}



package mqtt.storage;

import io.netty.util.CharsetUtil;

import java.util.Arrays;

/**
 * 用于接收转发的消息
 */
public class Message {
    /**
     * 消息id
     */
    private  int packetId;
    /**
     * 消息qos
     */
    private final int qos;
    /**
     * 消息 topic
     */
    private final String topic;
    /**
     * 消息内容
     */
    private final byte[] msg;

    /**
     * 消息占据的字节数， 读取时用到
     */
    private int size;

    public Message(String topic,byte[] msg,int qos){
        this.topic = topic;
        this.msg = msg;
        this.qos = qos;
    }
    public Message(int packetId, String topic, byte[] msg,int qos) {
        this.packetId = packetId;
        this.topic = topic;
        this.msg = msg;
        this.qos = qos;
    }

    public int getPacketId() {
        return packetId;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getMsg() {
        return msg;
    }
    static StoredMessage transToStoredMessage(Message msg){
        byte[] bytes = msg.topic.getBytes(CharsetUtil.UTF_8);
        return new StoredMessage(msg.getPacketId(),bytes.length,bytes,msg.msg.length,msg.msg,msg.qos);
    }

    int getSize() {
        return size;
    }

    void setSize(int size) {
        this.size = size;
    }

    public int getQos() {
        return qos;
    }

    public void setPacketId(int packetId) {
        this.packetId = packetId;
    }

    @Override
    public String toString() {
        return "Message{" +
                "packetId=" + packetId +
                ", qos=" + qos +
                ", topic='" + topic + '\'' +
                ", msg=" + Arrays.toString(msg) +
                '}';
    }
}



package mqtt.storage;

import io.netty.util.CharsetUtil;

/**
 * 用于存储的消息格式
 */
public class StoredMessage {
    /**
     * 两个字节
     */
    private final int packetId;
    /**
     * 1个字节
     */
    private final int qos;
    /**
     * 两个字节
     */
    private final int topicLen;

    private final byte[] topic;
    /**
     * 两个字节
     */
    private final int msgLen;
    private final byte[] msg;
    public StoredMessage(int packetId, int topicLen, byte[] topic, int msgLen, byte[] msg,int qos) {
        this.packetId = packetId;
        this.topicLen = topicLen;
        this.topic = topic;
        this.msgLen = msgLen;
        this.msg = msg;
        this.qos = qos;
    }

    public int getPacketId() {
        return packetId;
    }

    public int getTopicLen() {
        return topicLen;
    }

    public byte[] getTopic() {
        return topic;
    }

    public int getMsgLen() {
        return msgLen;
    }

    public int getQos() {
        return qos;
    }

    public byte[] getMsg() {
        return msg;
    }

    public static Message transToMessage(StoredMessage sM){
        Message msg = new Message(sM.getPacketId(),new String(sM.getTopic(), CharsetUtil.UTF_8),sM.getMsg(),sM.getQos());
        msg.setSize(sM.getBytesSize());
        return msg;
    }

    /**
     * 返回占用的byte字节数
     */
    public int getBytesSize(){
        return 7 + topicLen + msgLen;
    }
}

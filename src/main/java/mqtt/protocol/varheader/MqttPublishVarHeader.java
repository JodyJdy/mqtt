

package mqtt.protocol.varheader;

/**
 *发布报文 可变头部
 **/

public class MqttPublishVarHeader extends VaraibleHeader {
        private final String topicName;
        private final int packetId;

    public MqttPublishVarHeader(String topicName, int packetId) {
        this.topicName = topicName;
        this.packetId = packetId;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPacketId() {
        return packetId;
    }

    @Override
    public String toString() {
        return "MqttPublishVarHeader{" +
                "topicName='" + topicName + '\'' +
                ", packetId=" + packetId +
                '}';
    }
}

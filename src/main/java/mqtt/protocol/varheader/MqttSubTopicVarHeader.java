

package mqtt.protocol.varheader;


/**
 * 订阅报文 可变头部
 */
public class MqttSubTopicVarHeader extends VaraibleHeader {
    private final int packetId;

    public MqttSubTopicVarHeader(int packetId) {
        this.packetId = packetId;
    }

    public int getPacketId() {
        return packetId;
    }
}

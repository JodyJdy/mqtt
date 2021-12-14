

package mqtt.protocol.varheader;

/**
 * 发布确认报文 可变头部
 */
public class MqttPublishRecVarHeader extends VaraibleHeader {
    private final int packetId;

    public MqttPublishRecVarHeader(int packetId) {
        this.packetId = packetId;
    }

    public int getPacketId() {
        return packetId;
    }
}

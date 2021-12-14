

package mqtt.protocol.varheader;

/**
 * 发布完成报文  可变头部
 */
public class MqttPublishCompVarHeader extends VaraibleHeader {
    private final int packetId;

    public MqttPublishCompVarHeader(int packetId) {
        this.packetId = packetId;
    }

    public int getPacketId() {
        return packetId;
    }
}

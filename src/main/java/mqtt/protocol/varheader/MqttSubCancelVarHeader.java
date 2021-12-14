

package mqtt.protocol.varheader;

/**
 *定义取消报文 可变头部
 **/

public class MqttSubCancelVarHeader extends VaraibleHeader {
    private final int packetId;

    public MqttSubCancelVarHeader(int packetId) {
        this.packetId = packetId;
    }

    public int getPacketId() {
        return packetId;
    }
}



package mqtt.protocol.varheader;

/**
 *发布释放 报文 可变头部
 **/

public class MqttPublishRelVarHeader extends VaraibleHeader {
    private final int packetId;

    public MqttPublishRelVarHeader(int packetId) {

        this.packetId = packetId;
    }

    public int getPacketId() {
        return packetId;
    }
}



package mqtt.enums;

public enum MqttConnectReturnCode {

    /**
     * 0x00连接已接受
     */
    CONNECTION_ACCEPTED((byte) 0x00),
    /**
     * 0x01连接已拒绝，不支持的协议版本
     */
    CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION((byte) 0X01),
    /**
     * 0x02连接已拒绝，不合格的客户端标识符
     */
    CONNECTION_REFUSED_IDENTIFIER_REJECTED((byte) 0x02),
    /**
     * 0x03连接已拒绝，服务端不可用
     */
    CONNECTION_REFUSED_SERVER_UNAVAILABLE((byte) 0x03),
    /**
     * 0x04连接已拒绝，无效的用户名或密码
     */
    CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD((byte) 0x04),
    /**
     * 0x05连接已拒绝，未授权
     */
    CONNECTION_REFUSED_NOT_AUTHORIZED((byte) 0x05);

    private static final MqttConnectReturnCode[] VALUES;

    static {
        MqttConnectReturnCode[] values = values();
        VALUES = new MqttConnectReturnCode[160];
        for (MqttConnectReturnCode code : values) {
            final int unsignedByte = code.byteValue & 0xFF;
            VALUES[unsignedByte] = code;
        }
    }

    private final byte byteValue;

    MqttConnectReturnCode(byte byteValue) {
        this.byteValue = byteValue;
    }

    public byte byteValue() {
        return byteValue;
    }

    public static MqttConnectReturnCode valueOf(byte b) {
        final int unsignedByte = b & 0xFF;
        MqttConnectReturnCode mqttConnectReturnCode = null;
        try {
            mqttConnectReturnCode = VALUES[unsignedByte];
        } catch (ArrayIndexOutOfBoundsException ignored) {
        }
        return mqttConnectReturnCode;
    }
}

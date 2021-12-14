

package mqtt.enums;

/**
 * 消息类型枚举
 */
public enum MqttMessageType {
    /**
     * 连接服务端
     */
    CONNECT(1),
    /**
     * 确认连接请求
     */
    CONNACK(2),
    /**
     * 发布消息
     */
    PUBLISH(3),
    /**
     * 发布确认
     */
    PUBACK(4),
    /**
     * 发布接收
     */
    PUBREC(5),
    /**
     * 发布释放
     */
    PUBREL(6),
    /**
     * 发布完成
     */
    PUBCOMP(7),
    /**
     * 订阅
     */
    SUBSCRIBE(8),
    /**
     * 订阅确认
     */
    SUBACK(9),
    /**
     * 取消订阅
     */
    UNSUBSCRIBE(10),
    /**
     * 取消订阅确认
     */
    UNSUBACK(11),
    /**
     * 心跳请求
     */
    PINGREQ(12),
    /**
     * 心跳响应
     */
    PINGRESP(13),
    /**
     * 端口连接
     */
    DISCONNECT(14),
    /**
     * 认证
     */
    AUTH(15);

    private static final MqttMessageType[] VALUES;

    static {
        final MqttMessageType[] values = values();
        VALUES = new MqttMessageType[values.length + 1];
        for (MqttMessageType mqttMessageType : values) {
            final int value = mqttMessageType.value;
            if (VALUES[value] != null) {
                throw new AssertionError("value already in use: " + value);
            }
            VALUES[value] = mqttMessageType;
        }
    }

    private final int value;

    MqttMessageType(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static MqttMessageType valueOf(int type) {
        if (type <= 0 || type >= VALUES.length) {
            throw new IllegalArgumentException("unknown message type: " + type);
        }
        return VALUES[type];
    }
}

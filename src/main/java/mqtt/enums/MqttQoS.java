

package mqtt.enums;

/**
 * qos 枚举类
 */
public enum MqttQoS {
    /**
     * 至多一次
     */
    AT_MOST_ONCE(0),
    /**
     * 至少一次
     */
    AT_LEAST_ONCE(1),
    /**
     * 一次
     */
    EXACTLY_ONCE(2),
    /**
     * 失败
     */
    FAILURE(0x80);
    private final int value;

    MqttQoS(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static MqttQoS valueOf(int value) {
        switch (value) {
            case 0:
                return AT_MOST_ONCE;
            case 1:
                return AT_LEAST_ONCE;
            case 2:
                return EXACTLY_ONCE;
            case 0x80:
                return FAILURE;
            default:
                throw new IllegalArgumentException("invalid QoS: " + value);
        }
    }
}

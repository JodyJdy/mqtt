

package mqtt.protocol.varheader;

/**
 *
 * @date 2021/12/8
 **/

import io.netty.util.internal.StringUtil;

/**
 * 连接报文  可变头部
 */
public class MqttConnectVarHeader extends VaraibleHeader {
    private final String name;
    private final int version;
    private final boolean hasUserName;
    private final boolean hasPassword;
    private final boolean isWillRetain;
    private final int willQos;
    private final boolean isWillFlag;
    private final boolean isCleanSession;
    private final int keepAliveTimeSeconds;



    public MqttConnectVarHeader(
            String name,
            int version,
            boolean hasUserName,
            boolean hasPassword,
            boolean isWillRetain,
            int willQos,
            boolean isWillFlag,
            boolean isCleanSession,
            int keepAliveTimeSeconds) {
        this.name = name;
        this.version = version;
        this.hasUserName = hasUserName;
        this.hasPassword = hasPassword;
        this.isWillRetain = isWillRetain;
        this.willQos = willQos;
        this.isWillFlag = isWillFlag;
        this.isCleanSession = isCleanSession;
        this.keepAliveTimeSeconds = keepAliveTimeSeconds;
    }

    public String name() {
        return name;
    }

    public int version() {
        return version;
    }

    public boolean hasUserName() {
        return hasUserName;
    }

    public boolean hasPassword() {
        return hasPassword;
    }

    public boolean isWillRetain() {
        return isWillRetain;
    }

    public int willQos() {
        return willQos;
    }

    public boolean isWillFlag() {
        return isWillFlag;
    }

    public boolean isCleanSession() {
        return isCleanSession;
    }

    public int keepAliveTimeSeconds() {
        return keepAliveTimeSeconds;
    }


    @Override
    public String toString() {
        return new StringBuilder(StringUtil.simpleClassName(this))
                .append('[')
                .append("name=").append(name)
                .append(", version=").append(version)
                .append(", hasUserName=").append(hasUserName)
                .append(", hasPassword=").append(hasPassword)
                .append(", isWillRetain=").append(isWillRetain)
                .append(", isWillFlag=").append(isWillFlag)
                .append(", isCleanSession=").append(isCleanSession)
                .append(", keepAliveTimeSeconds=").append(keepAliveTimeSeconds)
                .append(']')
                .toString();
    }
}

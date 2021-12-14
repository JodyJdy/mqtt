

package mqtt.mqttclient;

/**
 *mqtt连接中的配置项
 **/

public class MqttConnectOptions {
    /**
     * 默认协议名
     */
    private static final String  PROTOCOL_NAME = "MQTT";
    /**
     * 默认版本号
     */
    private static final int VERSION = 3;
    /**
     * 默认保活时间
     */
    private static final int KEEP_ALIVE = 60;
    /**
     * 协议名称
     */
    private  String name = PROTOCOL_NAME;

    /**
     * 版本
     */
    private  int version = VERSION;
    private  boolean isWillRetain = false;
    private  int willQos = 0;
    private  boolean isWillFlag = false;
    private  boolean isCleanSession = true;
    private  int keepAliveTimeSeconds = KEEP_ALIVE;
    private  String clientIdentifier;
    private  String willTopic;
    private  byte[] willMessage;
    private  String userName;
    private  byte[] password;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean isWillRetain() {
        return isWillRetain;
    }

    public void setWillRetain(boolean willRetain) {
        isWillRetain = willRetain;
    }

    public int getWillQos() {
        return willQos;
    }

    public void setWillQos(int willQos) {
        this.willQos = willQos;
    }

    public boolean isWillFlag() {
        return isWillFlag;
    }

    public void setWillFlag(boolean willFlag) {
        isWillFlag = willFlag;
    }

    public boolean isCleanSession() {
        return isCleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        isCleanSession = cleanSession;
    }

    public int getKeepAliveTimeSeconds() {
        return keepAliveTimeSeconds;
    }

    public void setKeepAliveTimeSeconds(int keepAliveTimeSeconds) {
        this.keepAliveTimeSeconds = keepAliveTimeSeconds;
    }

    public String getClientIdentifier() {
        return clientIdentifier;
    }

    public void setClientIdentifier(String clientIdentifier) {
        this.clientIdentifier = clientIdentifier;
    }

    public String getWillTopic() {
        return willTopic;
    }

    public void setWillTopic(String willTopic) {
        this.willTopic = willTopic;
    }

    public byte[] getWillMessage() {
        return willMessage;
    }

    public void setWillMessage(byte[] willMessage) {
        this.willMessage = willMessage;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }
}

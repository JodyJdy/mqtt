import mqtt.mqttserver.MqttServer;

public class TestMqttServer {
    public  static void main(String[] args) {
        new MqttServer(9999).start();
    }
}

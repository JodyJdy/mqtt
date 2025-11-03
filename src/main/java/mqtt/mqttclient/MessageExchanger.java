

package mqtt.mqttclient;

import mqtt.storage.Message;
import mqtt.util.TopicUtil;

import java.util.Map;
import java.util.concurrent.*;

/**
 * 将消息中转到对应的 Listener处理
 *
 * @todo 如果多线程处理消息，会出现消息乱序，需要设置自定义逻辑，配置式的开启是否多线程
 */
public class MessageExchanger {
    private final ThreadPoolExecutor threadPoolExecutor;
    private final Map<String,MessageListener> subMap;
    MessageExchanger(Map<String, MessageListener> subMap){
        this.threadPoolExecutor =  new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
            Thread thread = new Thread(r);
            thread.setName("Message--Thread");
            return thread;
        });
        this.subMap  = subMap;
    }

    void shutDown(){
        threadPoolExecutor.shutdown();
    }

    public void submit(Message message){
        threadPoolExecutor.submit(()->{
            String topic = message.getTopic();
            subMap.get(topic).receiveMsg(message);
        });
    }


}

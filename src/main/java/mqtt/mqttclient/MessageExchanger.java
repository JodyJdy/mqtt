

package mqtt.mqttclient;

import mqtt.storage.Message;
import mqtt.util.TopicUtil;

import java.util.Map;
import java.util.concurrent.*;

/**
 * 将消息中转到对应的 Listener处理
 */
public class MessageExchanger {
    private final ThreadPoolExecutor threadPoolExecutor;
    private final Map<String,MessageListener> subMap;
    MessageExchanger(Map<String, MessageListener> subMap){
        this.threadPoolExecutor =  new ThreadPoolExecutor(4, 4, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
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
            subMap.forEach((k,v)->{
                if(TopicUtil.isMatch(k,topic)){
                    v.receiveMsg(message);
                }
            });
        });
    }


}

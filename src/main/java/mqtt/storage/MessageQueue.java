

package mqtt.storage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 消息中转队列
 */
public class MessageQueue {
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    public Message getMessage(){
        try {
            return queue.take();
        } catch (InterruptedException e) {
            return null;
        }
    }
    public void putMessage(Message msg){
        queue.add(msg);
    }

}



package mqtt.storage;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 消息中转队列
 */
public class MessageQueue {
    private final ConcurrentLinkedQueue<Message> queue = new ConcurrentLinkedQueue<>();
    public Message getMessage(){
            return queue.poll();
    }
    public int size(){
        return queue.size();
    }
    public void putMessage(Message msg){
        queue.add(msg);
    }

}

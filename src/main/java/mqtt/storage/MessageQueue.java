

package mqtt.storage;

/**
 * 消息中转队列
 */
public class MessageQueue {
    private final BlockingQueueWithoutLock<Message> queue = new BlockingQueueWithoutLock<>();
    public Message getMessage(){
        try {
            return queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
    public void putMessage(Message msg){
        queue.add(msg);
    }

}

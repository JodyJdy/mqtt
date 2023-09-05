

package mqtt.storage;

import mqtt.util.FileUtil;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * 消息中转队列
 */
public class MessageQueue {
    /**
     * 对大小做限制，防止oom
     */
    private final ArrayBlockingQueue<Message> queue = new ArrayBlockingQueue<>(FileUtil.BLOCKING_QUEUE_SIZE);

    public Message getMessage() throws InterruptedException {
        return queue.take();
    }

    public int size() {
        return queue.size();
    }

    public void putMessage(Message msg) {
        queue.add(msg);
    }

}

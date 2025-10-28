

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
    private final ArrayBlockingQueue<CallbackableMessage> queue = new ArrayBlockingQueue<>(FileUtil.BLOCKING_QUEUE_SIZE);

    public CallbackableMessage getMessage() throws InterruptedException {
        return queue.take();
    }

    public int size() {
        return queue.size();
    }

    public void addMessage(CallbackableMessage msg) {
        queue.add(msg);
    }

}

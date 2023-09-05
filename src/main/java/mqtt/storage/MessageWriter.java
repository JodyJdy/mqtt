

package mqtt.storage;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 将消息写入文件
 */
public class MessageWriter extends Thread {
    private final MessageQueue queue;
    private final MessageStorage messageStorage;
    public MessageWriter(MessageQueue queue, MessageStorage messageStorage){
        this.queue = queue;
        this.messageStorage = messageStorage;
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            try {
                Message msg = queue.getMessage();
                messageStorage.writeMessage(msg);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
    }
    }
}

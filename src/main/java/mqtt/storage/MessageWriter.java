

package mqtt.storage;

import java.io.IOException;

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
            Message msg = queue.getMessage();
            if (msg == null) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                continue;
            }
            try {
                messageStorage.writeMessage(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

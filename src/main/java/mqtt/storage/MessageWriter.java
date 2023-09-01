

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
        while(true){
            Message msg = queue.getMessage();
            try {
                if (msg == null) {
                    Thread.sleep(50);
                    continue;
                }
                messageStorage.writeMessage(msg);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

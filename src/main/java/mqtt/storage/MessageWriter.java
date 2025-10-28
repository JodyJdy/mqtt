

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
            try {
                CallbackableMessage msg = queue.getMessage();
                messageStorage.writeMessage(msg);
                //存储完毕后，执行回调，响应发送方
                msg.invokeCallback();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
    }
    }
}

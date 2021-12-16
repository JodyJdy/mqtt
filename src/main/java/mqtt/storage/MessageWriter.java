

package mqtt.storage;

import java.io.IOException;

/**
 * 将消息写入文件
 */
public class MessageWriter extends Thread {
    private final MessageQueue queue;
    private final FileProcess fileProcess;
    public MessageWriter(MessageQueue queue, FileProcess fileProcess){
        this.queue = queue;
        this.fileProcess = fileProcess;
    }

    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            Message msg = queue.getMessage();
            try {
                fileProcess.writeMessage(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

package mqtt.storage;

/**
 * 消息文件 写指针
 */
public class MessageFileWriterPointer {
    private volatile long writePos;
    private volatile int writeFile;


    public long getWritePos() {
        return writePos;
    }

    public void setWritePos(long writePos) {
        this.writePos = writePos;
    }

    public int getWriteFile() {
        return writeFile;
    }

    public void setWriteFile(int writeFile) {
        this.writeFile = writeFile;
    }

    public MessageFileWriterPointer() {
        this.writePos = 0;
        this.writeFile = 0;
    }

    public MessageFileWriterPointer(long writePos, int writeFile) {
        this.writePos = writePos;
        this.writeFile = writeFile;
    }
}

package mqtt.storage;

/**
 * 消息位置
 */
public class MessagePos {
    private final int fileIndex;

    public MessagePos(int fileIndex, long pos) {
        this.fileIndex = fileIndex;
        this.pos = pos;
    }

    private final long pos;

    public int getFileIndex() {
        return fileIndex;
    }

    public long getPos() {
        return pos;
    }
}

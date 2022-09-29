package mqtt.storage;

/**
 * 主题的索引文件读指针
 */
public class TopicIndexFileReaderPointer {
    /**
     * 主题名称
     */
    private final String topic;
    /**
     * 主题在 主题文件 中的位置
     */
    private final long topicPos;
    /**
     * 主题对应的索引文件 读取的索引
     */
    private final long readPos;

    public String getTopic() {
        return topic;
    }

    public long getTopicPos() {
        return topicPos;
    }

    public long getReadPos() {
        return readPos;
    }

    public TopicIndexFileReaderPointer(String topic, long readPos,long topicPos) {
        this.topic = topic;
        this.readPos = readPos;
        this.topicPos = topicPos;
    }
}

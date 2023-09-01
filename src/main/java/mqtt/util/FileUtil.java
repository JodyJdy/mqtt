package mqtt.util;

import java.io.File;

/**
 * @author jdy
 * @title: FileUtil
 * @description:
 * @data 2023/9/1 16:05
 */
public class FileUtil {
    /**
     * 最大文件大小, 50 MB
     */
    public static final int MAX_FILE_SIZE = 1024 * 1024 * 50;
    /**
     * 消息存储使用的文件格式
     */
    public final static String MSG_TYPE = ".msg";
    /**
     * 存储所有topic 文件
     */
    public final static String TOPIC_SET = "mqtt.topic";
    /**
     * 索引文件的文件格式
     */
    public final static String INDEX_TYPE = ".index";

    public final static String INDEX_POSITION = ".pos";
    /**
     * 文件写指针使用的文件名称
     */
    public final static String FILE_POINTER_NAME = "mqtt.pos";
    /**
     * 文件存储根路径
     */
    public final static String ROOT_PATH = "store/";
    /**
     * 文件索引路径
     */
    public final static String INDEX_PATH = ROOT_PATH + "index/";


    /**
     * @param index 消息文件下标
     */
    public static File getMessageFile(int index){
        return new File(ROOT_PATH + index + MSG_TYPE);
    }


    public static File getMessageFileWriterPointer(){
        return new File(ROOT_PATH + FILE_POINTER_NAME);
    }


    /**
     * 获取索引文件目录
     * @return
     */
    public static File getIndexFileDir(){
        return new File(INDEX_PATH);
    }


    /**
     * 一条消息索引的尺寸
     * @return
     */
    public static int getMessageIndexSize(){
        //
        return 8 + 4;
    }

    /**
     * 获取主题消息索引文件
     */
    public static File getIndexFile(String topic) {
        return  new File(INDEX_PATH + topic + INDEX_TYPE);
    }


    public static File getIndexFileReadPosFile(String topic){
        return  new File(INDEX_PATH + topic + INDEX_POSITION);
    }
}

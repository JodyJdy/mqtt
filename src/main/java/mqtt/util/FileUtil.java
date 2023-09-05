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
     * 磁盘页大小 4kb  4096 byte
     */
    public static final int PAGE_SIZE = 4 * 1024;


    /**
     * 中转队列大小
     */
    public static final int BLOCKING_QUEUE_SIZE = 100000;


    /**
     * 一次性读取的 消息索引的 数量
     */
    public static final int READ_MESSAGE_INDEX =  PAGE_SIZE - PAGE_SIZE% getMessageIndexSize();

    public static final int DEFAULT_SINGLE_FILE_SIZE = 50 * 1024 * 1024;
    /**
     * 最大文件大小, 1024 MB
     */
    public static final int MAX_FILE_SIZE = 1024 * 1024 * 1024;
    /**
     * 消息存储使用的文件格式
     */
    public final static String MSG_TYPE = ".msg";
    /**
     * 文件存储根路径
     */
    public final static String ROOT_PATH = "store/";
    /**
     * 文件索引路径
     */
    public final static String INDEX_PATH = ROOT_PATH + "index/";




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
        return 8;
    }

}

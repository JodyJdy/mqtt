package mqtt.storage;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.RandomAccessFile;

/**
 * 索引文件读取进度指针
 *
 * @author jdy
 * @title: IndexFileWriterPointer
 * @description:
 * @data 2023/9/1 15:57
 */
public class IndexFileReadPointer {

    /**
     * 进度文件
     */
    private final File file;
    /**
     * 索引文件下次应该读的指针
     */
    private volatile long readPos = 0;

    public IndexFileReadPointer(File file) {
        this.file = file;
        try {
            if (file.exists()) {
                RandomAccessFile ros = new RandomAccessFile(file, "rw");
                readPos = ros.readLong();
                ros.close();
            } else {
                FileUtils.touch(file);
            }
        } catch (Exception e) {
            System.out.println("创建/读取主题索引进度文件异常");
            System.exit(-1);
        }
    }

    public long getReadPos(){
        return readPos;
    }


    /**
     * 更新读取进度
     */
    public void updateReadPos(Long pos) {
        this.readPos = pos;
    }

    public void flush() {
        try {
            RandomAccessFile ros = new RandomAccessFile(file, "rw");
            ros.seek(0);
            ros.writeLong(readPos);
            ros.close();
        } catch (Exception e) {
            System.out.println("更新主题索引读取进度文件异常");
            System.exit(-1);
        }
    }
}

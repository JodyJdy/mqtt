

package mqtt.storage;


import mqtt.util.StorageUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 文件存储
 */
public class FileProcess {
    /**
     * 当前写入文件的文件下标
     */
    private int fileIndex;
    /**
     * 最大文件大小, 50 MB
     */
    private static int MAX_FILE_SIZE = 1024*1024 * 50 ;
    /**
     * 消息存储使用的文件格式
     */
    private final static String MSG_TYPE = ".msg";
    /**
     * 文件指针使用的文件名称
     */
    private final static String FILE_POINTER_NAME = "mqtt.pos";

    /**
     * 文件访问指针 读&写的
     */
    private final FilePointer filePointer;
    /**
     * 用来访问 FilePointer所在的文件
     */
    private RandomAccessFile pointerAccess;
    /**
     * 用来从文件读取消息
     */
    private RandomAccessFile reader;
    /**
     * 用来向文件写入数据，使用了内存文件映射，需要定时的刷盘
     */
    private MappedByteBuffer writer;

    /**
     * 文件读写信号量
     */
    private final FileOperationSemaphore fileOperationSemaphore;

    public FileProcess() throws IOException {

        File pointer = new File("store/" + FILE_POINTER_NAME);
        if (!pointer.exists()) {
            FileUtils.touch(pointer);
            pointerAccess = new RandomAccessFile(pointer, "rw");
            filePointer = new FilePointer(0, 0, 0, 0);
            writeFilePointer();
        } else {
            pointerAccess = new RandomAccessFile(pointer, "rw");
            filePointer = readFilePointer();
        }
        //更新文件索引
        fileIndex = (int) filePointer.getWriteFile();
        File readerAccessFile = new File("store/" + filePointer.getReadFile() + MSG_TYPE);
        File writerAccessFile = new File("store/" + filePointer.getWriteFile() + MSG_TYPE);
        if (!writerAccessFile.exists()) {
            FileUtils.touch(writerAccessFile);
        }
        RandomAccessFile temp = new RandomAccessFile(writerAccessFile, "rw");
        writer = temp.getChannel().map(FileChannel.MapMode.READ_WRITE, filePointer.getWritePos(), MAX_FILE_SIZE);
        if (!readerAccessFile.exists()) {
            throw new IOException("读取的文件不存在");
        }
        reader = new RandomAccessFile(readerAccessFile, "r");
        reader.seek(filePointer.getReadPos());
        //文件操作信号量
        this.fileOperationSemaphore = new FileOperationSemaphore(filePointer);
        //将文件指针FilePointer同步到文件中去
        ScheduledThreadPoolExecutor filePointerSyncer = new ScheduledThreadPoolExecutor(1, r -> new Thread("write-filePointer-thread"));
        filePointerSyncer.scheduleWithFixedDelay(this::writeFilePointer,2,2, TimeUnit.SECONDS);
        //将 内存中的数据刷盘,30s一次
        filePointerSyncer.scheduleAtFixedRate(()->writer.force(),5,30,TimeUnit.SECONDS);
    }

    /**
     * 向文件写数据
     */
    void writeMessage(Message msg) throws IOException {
        StoredMessage storedMessage = Message.transToStoredMessage(msg);
        writeMessage(storedMessage);
    }
    private void writeMessage(StoredMessage msg) throws IOException {
            long pos = filePointer.getWritePos();
            int size = msg.getBytesSize();
            //可以写入
            if(MAX_FILE_SIZE - pos >=size){
                filePointer.setWritePos(pos + size);
                StorageUtil.writeMessage(writer,msg);
                //标明可读
                fileOperationSemaphore.readble();
            } else{
                createWriter();
                writeMessage(msg);
            }
    }

    /**
     * 从文件里面读取消息
     */
    Message readMessage() throws IOException {
        //读取到栈里面
        long curWriterFile = filePointer.getWriteFile();
        long curReadFile = filePointer.getReadFile();
        long readPos = filePointer.getReadPos();
        //阻塞直到可以读取文件
        fileOperationSemaphore.waitUntilReadble();
        //第一个字节是长度，如果可读字节小于等于2，要切换文件
        boolean changeFile = MAX_FILE_SIZE - readPos <= 2;
        //读取packetId，如果为0说明读到文件的末端
        if (!changeFile) {
            int  packetId = (reader.readUnsignedByte() << 8) + reader.readUnsignedByte();
            //读取完后再将文件指针调整回去
            reader.seek(reader.getFilePointer() - 2);
            changeFile = packetId == 0;
        }
        //切换文件
        if (changeFile) {
            if (curReadFile == curWriterFile) {
                //无可读文件直接返回null
                return null;
            }
            createReader();
            return readMessage();
        }
        Message msg = StorageUtil.readMessage(reader);
        //更改文件指针位置
        filePointer.setReadPos(readPos + msg.getSize());
        return msg;
    }

    /**
     * 更换一个新的文件写数据
     */
    private void createWriter() throws IOException {
        synchronized (filePointer) {
            fileIndex++;
            File newFile = new File("store/" + fileIndex + MSG_TYPE);
            FileUtils.touch(newFile);
            RandomAccessFile randomAccessFile = new RandomAccessFile(newFile, "rw");
            writer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, MAX_FILE_SIZE);
            filePointer.setWritePos(0);
            filePointer.setWriteFile(fileIndex);
            //文件指针写入文件
            writeFilePointer();
        }
    }

    /**
     * 更换一个文件读取
     */
    private void createReader() throws FileNotFoundException {
        synchronized (filePointer) {
            long readFile = filePointer.getReadFile();
            if (readFile < fileIndex) {
                //读取下一个文件
                readFile++;
                File newReadFile = new File("store/" + readFile + MSG_TYPE);
                reader = new RandomAccessFile(newReadFile, "r");
                filePointer.setReadFile(readFile);
                filePointer.setReadPos(0);
            }
        }
    }

    /**
     * 写文件指针
     */
    private void writeFilePointer() {
        synchronized (filePointer) {
            try {
                pointerAccess.seek(0);
                pointerAccess.writeLong(filePointer.getReadFile());
                pointerAccess.writeLong(filePointer.getReadPos());
                pointerAccess.writeLong(filePointer.getWriteFile());
                pointerAccess.writeLong(filePointer.getWritePos());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 读文件指针
     */
    private FilePointer readFilePointer() throws IOException {
        pointerAccess.seek(0);
        return new FilePointer(pointerAccess.readLong(), pointerAccess.readLong(), pointerAccess.readLong(), pointerAccess.readLong());
    }

}

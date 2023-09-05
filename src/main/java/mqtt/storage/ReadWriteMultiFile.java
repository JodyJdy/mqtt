package mqtt.storage;

import mqtt.util.ByteUtil;
import mqtt.util.FileUtil;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 将内容写入到多个文件中
 * 读取多个文件表现的像是读取单文件
 *
 * @author jdy
 * @title: ReadWriteMultiFile
 * @description:
 * @data 2023/9/4 9:37
 */
public class ReadWriteMultiFile {


    /**
     *  writeFilePointer  force的频率
     *  默认1s一次
     */
    private final int pointerForcePeriod = 2;


    /**
     * writeFile force 的频率
     * 默认5s一次
     */
    private final int fileForcePeriod = 5;


    /**
     * 文件名称与序号的分隔符
     * xxx_0.filetype
     * xxx_1.filetype
     * xxx_2.filetype
     */
    private String split = "_";
    /**
     * 文件名称
     */
    private final String fileName;
    /**
     * 文件类型
     */
    private final String fileType;

    /**
     * 文件所处目录
     */
    private final String dir;

    /**
     * 当前写的文件下标
     */

    private volatile int writeFileIndex;

    /**
     * 当前写到的位置
     */
    private volatile long writeFilePos;

    /**
     * 单文件大小
     */
    private final int singleFileSize;


    /**
     * 当前写的位置
     */
    private final MappedByteBuffer writeFilePointer;

    /**
     * 当前在写的文件
     */
    private MappedByteBuffer writeFile;


    /**
     * 需要存储进度的 ReadPointer
     */
    private final ConcurrentHashMap<String, ReadPointer> needStoreProcessReadPointer = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MappedByteBuffer> needStoreProcess = new ConcurrentHashMap<>();


    public ReadWriteMultiFile(String fileName, String fileType, String dir) {
        this(fileName, fileType, dir, FileUtil.DEFAULT_SINGLE_FILE_SIZE);
    }

    /**
     * 文件第一次创建
     * 如果fileName == null, 文件名称格式为  1.fileType, 2.fileType ....
     */
    public ReadWriteMultiFile(String fileName, String fileType, String dir, int singleFileSize) {
        if (fileName == null || fileType == null) {
            throw new RuntimeException("文件名称/文件类型不能为空");
        }
        if (fileType.startsWith(".")) {
            fileType = fileType.substring(1);
        }
        this.fileType = fileType;
        this.fileName = fileName;
        this.singleFileSize = singleFileSize;
        this.dir = dir;
        writeFileIndex = 0;
        writeFilePos = 0;

        //判断文件是不是之前已经被创建了
        try {
            File pointerFile = FileUtils.getFile(dir, getWriterPointerFile());
            if (pointerFile.exists()) {
                RandomAccessFile pointer = new RandomAccessFile(pointerFile, "rw");
                singleFileSize = pointer.readInt();
                writeFileIndex = pointer.readInt();
                writeFilePos = pointer.readLong();
                writeFilePointer = pointer.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, getPointerFileSize());
            } else{
                FileUtils.touch(pointerFile);
                RandomAccessFile pointer = new RandomAccessFile(pointerFile, "rw");
                writeFilePointer = pointer.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, getPointerFileSize());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //创建写的文件
        try {
            File shouldWriteFile = FileUtils.getFile(dir, getFileNameWithIndex(writeFileIndex));
            if (shouldWriteFile.exists()) {
                FileUtils.touch(shouldWriteFile);
            }
            try (RandomAccessFile temp = new RandomAccessFile(FileUtils.getFile(dir, getFileNameWithIndex(writeFileIndex)), "rw")) {
                writeFile = temp.getChannel().map(FileChannel.MapMode.READ_WRITE, writeFilePos, singleFileSize);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        background();
    }


    /**
     * 启动后台任务
     */
    private void background() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        //文件写指针1s刷新一次
        executor.scheduleAtFixedRate(this::forceWritePos, 1, pointerForcePeriod, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(this::forceReadPointer, 1, pointerForcePeriod, TimeUnit.SECONDS);
        //将 内存中的数据刷盘,默认5s一次
        executor.scheduleAtFixedRate(() -> writeFile.force(), 0, fileForcePeriod, TimeUnit.SECONDS);
    }

    /**
     * 记录当前写的位置
     */
    private void forceWritePos(){
        writeFilePointer.position(0);
        writeFilePointer.putInt(singleFileSize);
        writeFilePointer.putInt(writeFileIndex);
        writeFilePointer.putLong(writeFilePos);
        writeFilePointer.force();
    }


    /**
     * 获取全局在写的位置
     * @return
     */
    public long getGlobalWritePos(){

        if (writeFileIndex == 0) {
            return writeFilePos;
        }
        return (long) writeFileIndex * singleFileSize + writeFilePos;
    }
    /**
     * 获取要读取的文件
     */
    public File getReadFileWithFileIndex(int fileIndex) {
        String fileName = getFileNameWithIndex(fileIndex);
        return FileUtils.getFile(dir, fileName);
    }

    /**
     * 获取 写指针文件
     * 用于记录文件写位置文件名称，为   .文件名.pos
     */
    public String getWriterPointerFile() {
        return "." + fileName + ".pos";
    }

    /**
     * 获取读写指针 文件的 大小
     * singleFileSize
     * writeFileIndex
     * writeFilePos
     */
    public int getPointerFileSize() {
        return 4 + 4 + 8;
    }

    /**
     * 获取文件名称
     */
    public String getFileNameWithIndex(int index) {
        return fileName + split + index + "." + fileType;
    }

    /**
     *返回进度的文件名称
     */
    public String getProcessName(String process) {
        return "." + process +  ".process";

    }

    public void put(byte b) {
        if (writeFile.remaining() == 0) {
            switchWriteFile();
        }
        writeFile.put(b);
        writeFilePos++;
    }

    public void put(byte[] src) {
        //有多少写多少
        if (writeFile.remaining() < src.length) {
            if (writeFile.remaining() == 0) {
                switchWriteFile();
                put(src);
                return;
            } else {
                int n = writeFile.remaining();
                put(src, 0, n);
                //这里会进行文件的切换
                put(src, n, src.length - n);
            }
            return;
        }
        writeFile.put(src);
        writeFilePos+=src.length;
    }

    public void put(byte[] src, int offset, int length) {
        if (writeFile.remaining() < length) {
            if (writeFile.remaining() == 0) {
                switchWriteFile();
                put(src, offset, length);
                return;
            } else {
                //有多少写多少
                int n = writeFile.remaining();
                put(src, offset, n);
                put(src, offset + n, length - n);
                return;
            }
        }
        writeFile.put(src, offset, length);
        writeFilePos+=length;
    }

    /**
     *为了处理方便，以及将所有字节都利用  所有基础数据类型， 不直接使用 putChar,putDouble,因为需要自己从字节拼接数据，
     * 直接使用 put(byte[] src);
     */
    public void put(char ch) {
        put(ByteUtil.char2Bytes(ch));
    }

    public void putDouble(double d) {
        put(ByteUtil.double2Bytes(d));
    }

    public void putLong(long d) {
        put(ByteUtil.long2Bytes(d));
    }

    public void putInt(int i) {
        put(ByteUtil.int2Bytes(i));
    }

    public void putFloat(float f) {
        put(ByteUtil.float2Bytes(f));
    }

    public void putShort(short s) {
        put(ByteUtil.short2Bytes(s));
    }

    /**
     * 切换到另一个文件写入
     */
    public synchronized void switchWriteFile() {
        try {
            writeFile.force();
            writeFileIndex++;
            writeFilePos = 0;
            File nextWriteFile = FileUtils.getFile(dir, getFileNameWithIndex(writeFileIndex));
            FileUtils.touch(nextWriteFile);
            try (RandomAccessFile temp = new RandomAccessFile(FileUtils.getFile(dir, getFileNameWithIndex(writeFileIndex)), "rw")) {
                writeFile = temp.getChannel().map(FileChannel.MapMode.READ_WRITE, writeFilePos, singleFileSize);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 从文件开始读取
     * @return
     */
    public RandomAccessReader getRandomAccessReader() {
        return new RandomAccessReader();
    }

    /**
     *手动传入进度
     * @param fileIndex  文件索引
     * @param pos 文件中的位置
     * @return
     */
    public RandomAccessReader getRandomAccessReader(int fileIndex, long pos) {
        return new RandomAccessReader(fileIndex, pos);
    }

    /**
     * 手动传入进度
     * @param globalPos 全局索引
     * @return
     */
    public RandomAccessReader getRandomAccessReader(long globalPos) {
        return new RandomAccessReader(globalPos);
    }

    /**
     * 从文件开始读取
     * @return
     */
    public InputStreamReader getInputStreamReader() {
        return new InputStreamReader();
    }

    /**
     *手动传入进度
     * @param fileIndex  文件索引
     * @param pos 文件中的位置
     * @return
     */
    public InputStreamReader getInputStreamReader(int fileIndex, long pos) {
        return new InputStreamReader(fileIndex, pos);
    }

    /**
     * 手动传入进度
     * @param globalPos 全局索引
     * @return
     */

    public InputStreamReader getInputStreamReader(long globalPos) {
        return new InputStreamReader(globalPos);
    }

    /**
     *如果需要存储 读取进度，需要 传入进度名称
     * 不能重复
     */
    public RandomAccessReader getRandomAccessReader(String processName) {
        if (needStoreProcessReadPointer.containsKey(processName)) {
            throw new RuntimeException("不能有重名的读取进度");
        }
        RandomAccessReader reader = new RandomAccessReader();
        initReadPointer(processName,reader);
        return reader;
    }

    public InputStreamReader getInputStreamReader(String processName) {
        if (needStoreProcessReadPointer.containsKey(processName)) {
            throw new RuntimeException("不能有重名的读取进度");
        }
        InputStreamReader reader = new InputStreamReader();
        initReadPointer(processName,reader);
        return reader;
    }


    public void initReadPointer(String processName,ReadPointer readPointer) {
        RandomAccessFile temp;
        try {
            File file = FileUtils.getFile(dir, getProcessName(processName));
            boolean couldRead = true;
            if (!file.exists()) {
                couldRead = false;
                FileUtils.touch(file);
            }
            temp = new RandomAccessFile(FileUtils.getFile(dir, getProcessName(processName)), "rw");
            MappedByteBuffer buffer = temp.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 8 + 4);
            needStoreProcessReadPointer.put(processName, readPointer);
            needStoreProcess.put(processName, buffer);
            //初始化进度
            if (couldRead) {
                readPointer.setReadPos(buffer.getLong());
                readPointer.setReadFileIndex(buffer.getInt());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void forceReadPointer(){
        needStoreProcessReadPointer.forEach((process,pointer)->{
            MappedByteBuffer buffer = needStoreProcess.get(process);
            buffer.position(0);
            buffer.putLong(pointer.getReadPos());
            buffer.putInt(pointer.getReadFileIndex());
            buffer.force();
        });
    }

    /**
     * 读指针
     */
    public interface ReadPointer{

        int getReadFileIndex();
        /**
         * 在下标为readFileIndex的文件中读取到的位置
         */
        long getReadPos();
        /**
         *  全局文件中 读取的位置
         */
        long getGlobalReadPos();


        void setReadFileIndex(int readFileIndex);

        void setReadPos(long readPos);

    }

    /**
     * 获取随机读取
     */
    public class RandomAccessReader implements ReadPointer {
        private volatile int  readFileIndex;
        /**
         * 在下标为readFileIndex的文件中读取到的位置
         */
        private long readPos;
        private RandomAccessFile readFile;


        public int getReadFileIndex() {
            return readFileIndex;
        }

        public long getReadPos() {
            return readPos;
        }

        @Override
        public long getGlobalReadPos() {
            if (readFileIndex == 0) {
                return readPos;
            }
            return (long) readFileIndex * singleFileSize + readPos;
        }

        /**
         * 传入全局的要读取的位置
         *
         * @param globalPos
         */
        public RandomAccessReader(long globalPos) {
            this.readFileIndex = (int) (globalPos / singleFileSize);
            this.readPos = (int) (globalPos % singleFileSize);
            try {
                readFile = new RandomAccessFile(getReadFileWithFileIndex(this.readFileIndex), "r");
                readFile.seek(readPos);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * 传入要读取文件的下标，和该文件读取位置
         */
        public RandomAccessReader(int readFileIndex, long readPos) {
            this.readFileIndex = readFileIndex;
            this.readPos = readPos;
            try {
                readFile = new RandomAccessFile(getReadFileWithFileIndex(this.readFileIndex), "r");
                readFile.seek(readPos);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void setReadFileIndex(int readFileIndex) {
            this.readFileIndex =readFileIndex;
        }

        @Override
        public void setReadPos(long readPos) {
            this.readPos = readPos;
        }

        public RandomAccessReader() {
            this(0, 0L);
        }

        public long length() {
            if (ReadWriteMultiFile.this.writeFileIndex == 0) {
                return ReadWriteMultiFile.this.writeFilePos;
            }
            return (long) ReadWriteMultiFile.this.writeFileIndex * singleFileSize + ReadWriteMultiFile.this.writeFilePos;
        }


        public void seek(long globalPos) throws IOException {
            //获取读取的文件的索引
            int before = this.readFileIndex;
            this.readFileIndex = (int) (globalPos / singleFileSize);
            this.readPos = (int) (globalPos % singleFileSize);
            //文件变更
            if (before != readFileIndex) {
                readFile = new RandomAccessFile(getReadFileWithFileIndex(this.readFileIndex), "r");
            }
            if (readFileIndex == ReadWriteMultiFile.this.writeFileIndex) {
                if (readPos > ReadWriteMultiFile.this.writeFilePos) {
                    throw new RuntimeException("pos越界");
                }
            }
            readFile.seek(readPos);
        }

        public long currentFileLength() {
            if (readFileIndex < ReadWriteMultiFile.this.writeFileIndex) {
                return singleFileSize;
            }
            return ReadWriteMultiFile.this.writeFilePos;
        }

        public byte readByte() throws IOException {
            long curLength = currentFileLength();
            if (readPos < curLength) {
                readPos++;
                return readFile.readByte();
            } else if (readFileIndex < ReadWriteMultiFile.this.writeFileIndex) {
                switchFile();
                return readByte();
            } else {
                throw new IOException("无可读取的数据");
            }
        }
        public int  readUnsignedByte() throws IOException {
            long curLength = currentFileLength();
            if (readPos < curLength) {
                readPos++;
                return readFile.read();
            } else if (readFileIndex < ReadWriteMultiFile.this.writeFileIndex) {
                switchFile();
                return readUnsignedByte();
            } else {
                throw new IOException("无可读取的数据");
            }

        }

        public int read(byte[] dst, int offset, int length) throws IOException {
            // 记录一开始要读取的长度
            int initLen = length;
            //顺序读取
            while (readFileIndex <= ReadWriteMultiFile.this.writeFileIndex) {
                long curLength = currentFileLength();
                //可以一次性读取完
                if (readPos + length <= curLength) {
                    readPos += length;
                    readFile.read(dst, offset, length);
                    length = 0;
                    break;
                } else {
                    //读取一部分，之后的内容从下个文件里面读取
                    int tempLen = (int) (curLength - readPos);
                    readFile.read(dst, offset, tempLen);
                    readPos += tempLen;
                    //调整偏移量
                    offset += tempLen;
                    //计算还要读取多少字节
                    length -= tempLen;
                }
                //已经是最后一个文件了，那么读到多少返回多少
                if (readFileIndex == ReadWriteMultiFile.this.writeFileIndex) {
                    break;
                }
                switchFile();
            }
            // 返回实际读取了多少
            return initLen - length;
        }

        public int read(byte[] dst) throws IOException {
            return read(dst, 0, dst.length);
        }

        public char readChar() throws IOException {
            byte[] bytes = new byte[2];
            read(bytes);
            return ByteUtil.bytes2Char(bytes);
        }

        public int readInt() throws IOException {
            byte[] bytes = new byte[4];
            read(bytes);
            return ByteUtil.bytes2Int(bytes);
        }

        public short readShort() throws IOException {
            byte[] bytes = new byte[2];
            read(bytes);
            return ByteUtil.bytes2Short(bytes);
        }

        public float readFloat() throws IOException {
            byte[] bytes = new byte[4];
            read(bytes);
            return ByteUtil.bytes2Float(bytes);
        }
        public double readDouble() throws IOException {
            byte[] bytes = new byte[8];
            read(bytes);
            return ByteUtil.bytes2Double(bytes);
        }

        public long readLong() throws IOException {
            byte[] bytes = new byte[8];
            read(bytes);
            return ByteUtil.bytes2Long(bytes);
        }

        public boolean readBoolean() throws IOException {
            return readByte() != 0;
        }

        private synchronized void switchFile() {
            if (this.readFileIndex == ReadWriteMultiFile.this.writeFileIndex) {
                throw new RuntimeException("不存在未读取的文件");
            }
            this.readFileIndex++;
            this.readPos = 0;
            try {
                readFile = new RandomAccessFile(getReadFileWithFileIndex(this.readFileIndex), "r");
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 获取顺序读
     */
    public class InputStreamReader implements ReadPointer {
        /**
         * 读取的文件的下标
         */
        private volatile int readFileIndex;
        /**
         * 在下标为readFileIndex的文件中读取到的位置
         */
        private volatile long readPos;
        private InputStream readFile;


        public int getReadFileIndex() {
            return readFileIndex;
        }

        public long getReadPos() {
            return readPos;
        }

        /**
         * 传入全局的要读取的位置
         *
         * @param globalPos
         */
        public InputStreamReader(long globalPos) {
            this.readFileIndex = (int) (globalPos / singleFileSize);
            this.readPos = (int) (globalPos % singleFileSize);
            try {
                readFile = new BufferedInputStream(Files.newInputStream(getReadFileWithFileIndex(this.readFileIndex).toPath()));
                readFile.skip(readPos);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * 传入要读取文件的下标，和该文件读取位置
         */
        public InputStreamReader(int readFileIndex, long readPos) {
            this.readFileIndex = readFileIndex;
            this.readPos = readPos;
            try {
                readFile = new BufferedInputStream(Files.newInputStream(getReadFileWithFileIndex(this.readFileIndex).toPath()));
                readFile.skip(readPos);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public InputStreamReader() {
            this(0, 0L);
        }


        /**
         *只能向前
         */
        public void seek(long globalPos) throws IOException {
            //获取读取的文件的索引
            int before = this.readFileIndex;
            this.readFileIndex = (int) (globalPos / singleFileSize);
            this.readPos = (int) (globalPos % singleFileSize);
            //文件变更
            if (before != readFileIndex) {
                readFile = new BufferedInputStream(Files.newInputStream(getReadFileWithFileIndex(this.readFileIndex).toPath()));
            }
            if (readFileIndex == ReadWriteMultiFile.this.writeFileIndex) {
                if (readPos > ReadWriteMultiFile.this.writeFilePos) {
                    throw new RuntimeException("pos越界");
                }
            }
            if(readFile.skip(readPos) != readPos){
                throw new RuntimeException("pos越界");
            }
        }

        public long currentFileLength() {
            if (readFileIndex < ReadWriteMultiFile.this.writeFileIndex) {
                return singleFileSize;
            }
            return ReadWriteMultiFile.this.writeFilePos;
        }

        public int readByte() throws IOException {
            long curLength = currentFileLength();
            if (readPos < curLength) {
                readPos++;
                return readFile.read();
            } else if (readFileIndex < ReadWriteMultiFile.this.writeFileIndex) {
                switchFile();
                return readByte();
            } else {
                throw new IOException("无可读取的数据");
            }
        }

        public int read(byte[] dst, int offset, int length) throws IOException {
            // 记录一开始要读取的长度
            int initLen = length;
            //顺序读取
            while (readFileIndex <= ReadWriteMultiFile.this.writeFileIndex) {
                long curLength = currentFileLength();
                //可以一次性读取完
                if (readPos + length <= curLength) {
                    readPos += readFile.read(dst, offset, length);
                    length = 0;
                    break;
                } else {
                    //读取一部分，之后的内容从下个文件里面读取
                    int tempLen = (int) (curLength - readPos);
                    tempLen = readFile.read(dst, offset, tempLen);
                    readPos += tempLen;
                    //调整偏移量
                    offset += tempLen;
                    //计算还要读取多少字节
                    length -= tempLen;
                }
                //已经是最后一个文件了，那么读到多少返回多少
                if (readFileIndex == ReadWriteMultiFile.this.writeFileIndex) {
                    break;
                }
                switchFile();
            }
            // 返回实际读取了多少
            return initLen - length;
        }

        public int read(byte[] dst) throws IOException {
            return read(dst, 0, dst.length);
        }

        @Override
        public long getGlobalReadPos() {
            if (readFileIndex == 0) {
                return readPos;
            }
            return (long) readFileIndex * singleFileSize + readPos;
        }

        private synchronized void switchFile() {
            if (this.readFileIndex == ReadWriteMultiFile.this.writeFileIndex) {
                throw new RuntimeException("不存在未读取的文件");
            }
            this.readFileIndex++;
            this.readPos = 0;
            try {
                readFile = new BufferedInputStream(Files.newInputStream(getReadFileWithFileIndex(this.readFileIndex).toPath()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void setReadFileIndex(int readFileIndex) {
            this.readFileIndex =readFileIndex;
        }

        @Override
        public void setReadPos(long readPos) {
            this.readPos = readPos;
        }
    }
}

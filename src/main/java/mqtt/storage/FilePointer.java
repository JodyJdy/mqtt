

package mqtt.storage;

/**
 * 文件指针，描述当前文件读取/写入位置
 */
@Deprecated
public class FilePointer {

    private volatile long readPos;
    private volatile long readFile;
    private volatile long writePos;
    private volatile long writeFile;

    FilePointer(long readFile, long readPos, long writeFile, long writePos) {
        this.readPos = readPos;
        this.readFile = readFile;
        this.writePos = writePos;
        this.writeFile = writeFile;
    }

    long getReadPos() {
        return readPos;
    }

    void setReadPos(long readPos) {
        this.readPos = readPos;
    }

    long getReadFile() {
        return readFile;
    }

    void setReadFile(long readFile) {
        this.readFile = readFile;
    }

    long getWritePos() {
        return writePos;
    }

    void setWritePos(long writePos) {
        this.writePos = writePos;
    }

    long getWriteFile() {
        return writeFile;
    }

    void setWriteFile(long writeFile) {
        this.writeFile = writeFile;
    }
    @Override
    public String toString() {
        return "FilePointer{" +
                "readPos=" + readPos +
                ", readFile=" + readFile +
                ", writePos=" + writePos +
                ", writeFile=" + writeFile +
                '}';
    }
}

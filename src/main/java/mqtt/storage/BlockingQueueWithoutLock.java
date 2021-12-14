package mqtt.storage;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

/**
 * BlockingQueue，添加-获取元素都要获取lock， 这里使用并发队列，无加解锁
 * 引入轻量级的semaphore来代替锁的功能。在获取不到元素时进行阻塞
 * @param <T>
 */
class BlockingQueueWithoutLock<T>  {
    private ConcurrentLinkedQueue<T> internelQueue = new ConcurrentLinkedQueue<>();

    private Semaphore semaphore = new Semaphore(0);

    void add(T s) {
        internelQueue.add(s);
        semaphore.release(1);
    }


    T take() throws InterruptedException {
        semaphore.acquire(1);
        return internelQueue.poll();
    }

}

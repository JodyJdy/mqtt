package mqtt.util;

/**
 * @author jdy
 * @title: Pair
 * @description:
 * @data 2023/9/1 17:33
 */
public class Pair <K,V>{
    private final K k;
    private final V v;

    public Pair(K k, V v) {
        this.k = k;
        this.v = v;
    }

    public K getK() {
        return k;
    }

    public V getV() {
        return v;
    }

    @Override
    public String toString() {
        return "k=" +k + ",v="+v;
    }

    public static <K, V> Pair<K, V> create(K k, V v) {
        return new Pair<>(k, v);
    }
}

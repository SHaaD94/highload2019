package not.mine.unused;

public interface ICache {
    byte[] get(long key);
    boolean put(long key, byte[] value);
    void close();
}
package kvstore;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class KVStore 
{

    private final Map<String, VersionedValue> data = new ConcurrentHashMap<>();
    private final WriteAheadLog wal;
    private final ReentrantLock writeLock = new ReentrantLock();

    public KVStore(String walFilePath) throws IOException 
    {
        this.wal = new WriteAheadLog(walFilePath);
        recover();
    }

    private void recover() throws IOException 
    {
        wal.replay(data::put); 
        System.out.println("Recovered " + data.size() + " keys from WAL.");
    }

    public String get(String key) 
    {
        VersionedValue v = data.get(key);
        if (v == null || v.tombstone)
        {
            return null;
        }
        return v.value;
    }

    public VersionedValue getVersioned(String key) 
    {
        return data.get(key);
    }

    public VersionedValue put(String key, String value) throws IOException 
    {
        VersionedValue v = VersionedValue.of(value);
        applyLocally(key, v);
        return v;
    }

    public VersionedValue delete(String key) throws IOException 
    {
        VersionedValue v = VersionedValue.tombstoneNow();
        applyLocally(key, v);
        return v;
    }

    public boolean applyReplicated(String key, VersionedValue incoming) throws IOException 
    {
        writeLock.lock();
        try {
            VersionedValue current = data.get(key);
            if (incoming.isNewerThan(current)) 
            {
                wal.logSet(key, incoming);
                data.put(key, incoming);
                return true;
            }
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    private void applyLocally(String key, VersionedValue v) throws IOException 
    {
        writeLock.lock();
        try {
            wal.logSet(key, v); 
            data.put(key, v);   
        } finally {
            writeLock.unlock();
        }
    }

    public int size() 
    {
        return data.size();
    }

    public void close() throws IOException 
    {
        wal.close();
    }
}

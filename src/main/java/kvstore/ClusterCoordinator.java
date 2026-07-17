package kvstore;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

public class ClusterCoordinator 
{

    private final NodeInfo self;
    private final KVStore localStore;
    private final ConsistentHashRing ring;
    private final FailureDetector failureDetector;
    private final int replicationFactor; 
    private final int writeQuorum;       
    private final int readQuorum;        
    private final ExecutorService pool = Executors.newFixedThreadPool(16);

    public ClusterCoordinator(NodeInfo self, KVStore localStore, ConsistentHashRing ring,
                               FailureDetector failureDetector, int n, int w, int r) {
        this.self = self;
        this.localStore = localStore;
        this.ring = ring;
        this.failureDetector = failureDetector;
        this.replicationFactor = n;
        this.writeQuorum = w;
        this.readQuorum = r;
    }

    public static class QuorumException extends Exception 
    {
        public QuorumException(String msg)
        {
            super(msg);
        }
    }

    public VersionedValue put(String key, String value) throws QuorumException 
    {
        return writeInternal(key, VersionedValue.of(value));
    }

    public VersionedValue delete(String key) throws QuorumException 
    {
        return writeInternal(key, VersionedValue.tombstoneNow());
    }

    private VersionedValue writeInternal(String key, VersionedValue v) throws QuorumException 
    {
        List<NodeInfo> targets = alivePreferenceList(key);
        if (targets.size() < writeQuorum) 
        {
            throw new QuorumException("Not enough alive nodes for write quorum: need " + writeQuorum
                    + ", only " + targets.size() + " alive");
        }

        List<Future<Boolean>> futures = new java.util.ArrayList<>();
        for (NodeInfo node : targets) 
        {
            futures.add(pool.submit(() -> sendWrite(node, key, v)));
        }

        int acks = 0;
        for (Future<Boolean> f : futures) 
        {
            try {
                if (f.get(1, TimeUnit.SECONDS)) acks++;
            } catch (Exception e) {
                
            }
        }

        if (acks < writeQuorum) 
        {
            throw new QuorumException("Write quorum not reached: got " + acks + " acks, needed " + writeQuorum);
        }
        return v;
    }

    private boolean sendWrite(NodeInfo node, String key, VersionedValue v) 
    {
        if (node.id().equals(self.id())) 
        {
            try {
                localStore.applyReplicated(key, v);
                return true;
            } catch (IOException e) {
                return false;
            }
        }
        HttpUtil.Response resp = HttpUtil.put(node.baseUrl() + "/internal/put/" + key, v.value, v.timestamp, v.tombstone);
        return !resp.failed() && resp.statusCode() == 200;
    }

    public String get(String key) throws QuorumException 
    {
        List<NodeInfo> targets = alivePreferenceList(key);
        if (targets.size() < readQuorum) 
        {
            throw new QuorumException("Not enough alive nodes for read quorum: need " + readQuorum
                    + ", only " + targets.size() + " alive");
        }

        List<Future<VersionedValue>> futures = new java.util.ArrayList<>();
        int toQuery = Math.min(targets.size(), Math.max(readQuorum, 1));
        for (int i = 0; i < toQuery; i++) 
        {
            NodeInfo node = targets.get(i);
            futures.add(pool.submit(() -> fetchVersion(node, key)));
        }

        VersionedValue best = null;
        int responded = 0;
        for (Future<VersionedValue> f : futures) 
        {
            try {
                VersionedValue v = f.get(1, TimeUnit.SECONDS); 
                responded++;
                if (v != null && v.isNewerThan(best)) 
                {
                    best = v;
                }
            } catch (Exception e) {
                
            }
        }

        if (responded < readQuorum) 
        {
            throw new QuorumException("Read quorum not reached: got " + responded + " responses, needed " + readQuorum);
        }

        final VersionedValue winner = best;
        if (winner != null) 
        {
            for (NodeInfo node : targets.subList(0, toQuery)) 
            {
                pool.submit(() -> sendWrite(node, key, winner));
            }
        }

        if (best == null || best.tombstone) return null;
        return best.value;
    }

    private VersionedValue fetchVersion(NodeInfo node, String key) 
    {
        if (node.id().equals(self.id())) 
        {
            return localStore.getVersioned(key);
        }
        HttpUtil.Response resp = HttpUtil.get(node.baseUrl() + "/internal/get/" + key);
        if (resp.failed() || resp.statusCode() == 404)
        {
            return null;
        }
        if (resp.statusCode() != 200)
        {
            return null;
        }

        String[] parts = resp.body().split("\\|", 3);
        long ts = Long.parseLong(parts[0]);
        boolean tombstone = parts[1].equals("1");
        String value = parts.length > 2 ? parts[2] : "";
        return new VersionedValue(value, ts, tombstone);
    }

    private List<NodeInfo> alivePreferenceList(String key) 
    {
        List<NodeInfo> preference = ring.getPreferenceList(key, replicationFactor);
        return preference.stream().filter(n -> failureDetector.isAlive(n.id())).toList();
    }

    public void applyInternalWrite(String key, VersionedValue v) throws IOException 
    {
        localStore.applyReplicated(key, v);
    }

    public VersionedValue readLocal(String key) 
    {
        return localStore.getVersioned(key);
    }
}

package kvstore;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;


public class ConsistentHashRing 
{
    private static final int VIRTUAL_NODES_PER_PHYSICAL = 100;

    private final TreeMap<Long, NodeInfo> ring = new TreeMap<>();

    public synchronized void addNode(NodeInfo node) 
    {
        for (int i = 0; i < VIRTUAL_NODES_PER_PHYSICAL; i++) 
        {
            long pos = hash(node.id() + "#" + i);
            ring.put(pos, node);
        }
    }

    public synchronized void removeNode(String nodeId) 
    {
        ring.entrySet().removeIf(e -> e.getValue().id().equals(nodeId));
    }

    public synchronized List<NodeInfo> getPreferenceList(String key, int count) 
    {
        List<NodeInfo> result = new ArrayList<>();
        if (ring.isEmpty())
        {
            return result;
        }

        long keyHash = hash(key);
        Set<String> seenNodeIds = new HashSet<>();

        SortedMap<Long, NodeInfo> tail = ring.tailMap(keyHash);
        Iterator<Long> it = tail.keySet().iterator();
        boolean wrapped = false;

        while (result.size() < count && result.size() < ring.size() / VIRTUAL_NODES_PER_PHYSICAL + 1) 
        {
            if (!it.hasNext()) 
            {
                if (wrapped)
                {
                    break;
                } 
                it = ring.keySet().iterator(); 
                wrapped = true;
            }
            if (!it.hasNext())
            {
                break; 
            }
            long pos = it.next();
            NodeInfo node = ring.get(pos);
            if (seenNodeIds.add(node.id())) 
            {
                result.add(node);
            }
            if (seenNodeIds.size() >= distinctPhysicalNodeCount())
            {
                break; 
            }
        }
        return result;
    }

    private int distinctPhysicalNodeCount() 
    {
        Set<String> ids = new HashSet<>();
        for (NodeInfo n : ring.values()) ids.add(n.id());
        return ids.size();
    }

    private long hash(String input) 
    {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = md5.digest(input.getBytes());
            
            long h = 0;
            for (int i = 0; i < 8; i++) 
            {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return Math.abs(h);
        } catch (NoSuchAlgorithmException e) 
        {
            throw new RuntimeException(e);
        }
    }
}

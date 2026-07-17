package kvstore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FailureDetector 
{
    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private static final long DEAD_TIMEOUT_MS = 3000; 

    private final NodeInfo self;
    private final List<NodeInfo> peers; 
    private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public FailureDetector(NodeInfo self, List<NodeInfo> allNodes) 
    {
        this.self = self;
        this.peers = allNodes.stream().filter(n -> !n.id().equals(self.id())).toList();
        long now = System.currentTimeMillis();
        for (NodeInfo p : peers) 
        {
            lastSeen.put(p.id(), now); 
        }
    }

    public void start() 
    {
        scheduler.scheduleAtFixedRate(this::pingAllPeers, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void pingAllPeers() 
    {
        for (NodeInfo peer : peers) 
        {
            HttpUtil.Response resp = HttpUtil.get(peer.baseUrl() + "/internal/health");
            if (!resp.failed() && resp.statusCode() == 200) 
            {
                lastSeen.put(peer.id(), System.currentTimeMillis());
            }
        }
    }

    public boolean isAlive(String nodeId) 
    {
        if (nodeId.equals(self.id()))
        {
            return true;
        }
        Long seen = lastSeen.get(nodeId);
        if (seen == null)
        {
            return false;
        }
        return (System.currentTimeMillis() - seen) < DEAD_TIMEOUT_MS;
    }

    public void stop() 
    {
        scheduler.shutdownNow();
    }
}

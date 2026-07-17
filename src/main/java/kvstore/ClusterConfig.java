package kvstore;

import java.util.ArrayList;
import java.util.List;

public class ClusterConfig 
{
    public static List<NodeInfo> parse(String spec) 
    {
        List<NodeInfo> nodes = new ArrayList<>();
        for (String entry : spec.split(",")) 
        {
            entry = entry.trim();
            if (entry.isEmpty())
            {
                continue;
            }
            String[] idAndAddr = entry.split("=");
            String id = idAndAddr[0].trim();
            String[] hostPort = idAndAddr[1].trim().split(":");
            String host = hostPort[0];
            int port = Integer.parseInt(hostPort[1]);
            nodes.add(new NodeInfo(id, host, port));
        }
        return nodes;
    }
}

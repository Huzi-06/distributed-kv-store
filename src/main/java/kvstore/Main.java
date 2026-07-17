package kvstore;

import java.io.IOException;
import java.util.List;

public class Main 
{
    public static void main(String[] args) throws IOException 
    {
        if (args.length < 4) 
        {
            System.err.println("Usage: java -jar kvstore.jar <nodeId> <port> <walPath> <clusterSpec> [N] [W] [R]");
            System.exit(1);
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);
        String walPath = args[2];
        String clusterSpec = args[3];
        int n = args.length > 4 ? Integer.parseInt(args[4]) : 3;
        int w = args.length > 5 ? Integer.parseInt(args[5]) : 2;
        int r = args.length > 6 ? Integer.parseInt(args[6]) : 2;

        List<NodeInfo> allNodes = ClusterConfig.parse(clusterSpec);
        NodeInfo self = allNodes.stream()
                .filter(node -> node.id().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("nodeId '" + nodeId + "' not found in cluster spec"));

        ConsistentHashRing ring = new ConsistentHashRing();
        for (NodeInfo node : allNodes)
        {
            ring.addNode(node);
        }

        FailureDetector failureDetector = new FailureDetector(self, allNodes);
        failureDetector.start();

        KVStore store = new KVStore(walPath);
        ClusterCoordinator coordinator = new ClusterCoordinator(self, store, ring, failureDetector, n, w, r);
        Server server = new Server(coordinator, port);
        server.start();

        System.out.println("Node '" + nodeId + "' up. Cluster: " + allNodes + " | N=" + n + " W=" + w + " R=" + r);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down node '" + nodeId + "'...");
            server.stop();
            failureDetector.stop();
            try 
            { 
                store.close(); 
            } 
            catch (IOException e) 
            { 
                e.printStackTrace(); 
            }

        }));
    }
}

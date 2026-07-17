package kvstore;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

public class Server 
{

    private final ClusterCoordinator coordinator;
    private final int port;
    private HttpServer httpServer;

    public Server(ClusterCoordinator coordinator, int port) 
    {
        this.coordinator = coordinator;
        this.port = port;
    }

    public void start() throws IOException 
    {
        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.createContext("/key/", new ClientKeyHandler());
        httpServer.createContext("/internal/health", exchange -> respond(exchange, 200, "OK"));
        httpServer.createContext("/internal/put/", new InternalPutHandler());
        httpServer.createContext("/internal/get/", new InternalGetHandler());
        httpServer.setExecutor(Executors.newFixedThreadPool(16));
        httpServer.start();
        System.out.println("Node listening on port " + port);
    }

    public void stop() 
    {
        if (httpServer != null)
        {
            httpServer.stop(0);
        }
    }

    private class ClientKeyHandler implements HttpHandler 
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException 
        {
            String key = exchange.getRequestURI().getPath().substring("/key/".length());
            if (key.isBlank()) 
            { 
                respond(exchange, 400, "Missing key"); 
                return; 
            }

            try {
                switch (exchange.getRequestMethod()) 
                {
                    case "GET" -> {
                        String value = coordinator.get(key);
                        if (value == null)
                        {
                            respond(exchange, 404, "Key not found");
                        }
                        else
                        {
                            respond(exchange, 200, value);
                        }
                    }
                    case "PUT" -> {
                        String value = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                        coordinator.put(key, value);
                        respond(exchange, 200, "OK");
                    }
                    case "DELETE" -> {
                        coordinator.delete(key);
                        respond(exchange, 200, "OK");
                    }
                    default -> respond(exchange, 405, "Method not allowed");
                }
            } catch (ClusterCoordinator.QuorumException e) 
            {
                respond(exchange, 503, "Quorum not reached: " + e.getMessage());
            }
        }
    }

    private class InternalPutHandler implements HttpHandler 
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException 
        {
            String key = exchange.getRequestURI().getPath().substring("/internal/put/".length());
            String value = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            long ts = Long.parseLong(exchange.getRequestHeaders().getFirst("X-Timestamp"));
            boolean tombstone = "1".equals(exchange.getRequestHeaders().getFirst("X-Tombstone"));
            coordinator.applyInternalWrite(key, new VersionedValue(value, ts, tombstone));
            respond(exchange, 200, "OK");
        }
    }

    private class InternalGetHandler implements HttpHandler 
    {
        @Override
        public void handle(HttpExchange exchange) throws IOException 
        {
            String key = exchange.getRequestURI().getPath().substring("/internal/get/".length());
            VersionedValue v = coordinator.readLocal(key);
            if (v == null) 
            { 
                respond(exchange, 404, "Not found"); 
                return; 
            }
            String body = v.timestamp + "|" + (v.tombstone ? "1" : "0") + "|" + v.value;
            respond(exchange, 200, body);
        }
    }

    private void respond(HttpExchange exchange, int statusCode, String body) throws IOException 
    {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}

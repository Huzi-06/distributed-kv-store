package kvstore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.*;


public class Benchmark 
{
    public static void main(String[] args) throws Exception 
    {
        if (args.length < 3) 
        {
            System.err.println("Usage: java -cp target/classes kvstore.Benchmark <baseUrl> <numRequests> <concurrency>");
            System.exit(1);
        }

        String baseUrl = args[0];
        int numRequests = Integer.parseInt(args[1]);
        int concurrency = Integer.parseInt(args[2]);

        HttpClient client = HttpClient.newHttpClient();
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        List<Future<Long>> futures = new ArrayList<>();

        long start = System.nanoTime();

        for (int i = 0; i < numRequests; i++) 
        {
            final int idx = i;
            futures.add(pool.submit(() -> {
                String key = "bench-key-" + (idx % 100); 
                String value = "value-" + idx;
                long t0 = System.nanoTime();
                HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/key/" + key))
                        .PUT(HttpRequest.BodyPublishers.ofString(value))
                        .timeout(java.time.Duration.ofSeconds(2))
                        .build();
                try {
                    HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
                    if (resp.statusCode() != 200) 
                    {
                        return -1L; 
                    }
                } catch (Exception e) {
                    return -1L; 
                }
                return (System.nanoTime() - t0) / 1_000_000; // latency in ms
            }));
        }

        List<Long> latencies = new ArrayList<>();
        int failures = 0;
        for (Future<Long> f : futures) 
        {
            long latency = f.get();
            if (latency < 0)
            {
                failures++;
            } 
            else
            {
                latencies.add(latency);
            } 
        }

        long totalMs = (System.nanoTime() - start) / 1_000_000;
        Collections.sort(latencies);

        System.out.println("=== Benchmark results ===");
        System.out.println("Total requests:   " + numRequests);
        System.out.println("Failures:         " + failures);
        System.out.println("Total time:       " + totalMs + " ms");
        System.out.printf("Throughput:       %.1f req/sec%n", numRequests / (totalMs / 1000.0));
        if (!latencies.isEmpty()) {
            System.out.println("Latency p50:      " + percentile(latencies, 50) + " ms");
            System.out.println("Latency p95:      " + percentile(latencies, 95) + " ms");
            System.out.println("Latency p99:      " + percentile(latencies, 99) + " ms");
        }

        pool.shutdown();
    }

    private static long percentile(List<Long> sortedLatencies, int p) 
    {
        int index = (int) Math.ceil(p / 100.0 * sortedLatencies.size()) - 1;
        index = Math.max(0, Math.min(index, sortedLatencies.size() - 1));
        return sortedLatencies.get(index);
    }
}
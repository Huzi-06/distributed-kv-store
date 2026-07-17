package kvstore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class HttpUtil 
{

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500))
            .build();

    public record Response(int statusCode, String body, boolean failed) 
    {
        public static Response failure() 
        {
            return new Response(-1, null, true);
        }
    }

    public static Response get(String url) 
    {
        return send(HttpRequest.newBuilder(URI.create(url)).GET());
    }

    public static Response put(String url, String body, long timestamp, boolean tombstone) 
    {
        return send(HttpRequest.newBuilder(URI.create(url))
                .header("X-Timestamp", Long.toString(timestamp))
                .header("X-Tombstone", tombstone ? "1" : "0")
                .PUT(HttpRequest.BodyPublishers.ofString(body)));
    }

    private static Response send(HttpRequest.Builder builder) 
    {
        try {
            HttpRequest request = builder.timeout(Duration.ofMillis(800)).build();
            HttpResponse<String> resp = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(resp.statusCode(), resp.body(), false);
        } catch (Exception e) {
            return Response.failure();
        }
    }
}

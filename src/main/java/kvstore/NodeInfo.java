package kvstore;

public record NodeInfo(String id, String host, int port) 
{
    public String baseUrl() 
    {
        return "http://" + host + ":" + port;
    }
}

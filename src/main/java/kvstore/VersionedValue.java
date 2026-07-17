package kvstore;

public class VersionedValue 
{
    public final String value;
    public final long timestamp;
    public final boolean tombstone;

    public VersionedValue(String value, long timestamp, boolean tombstone) 
    {
        this.value = value;
        this.timestamp = timestamp;
        this.tombstone = tombstone;
    }

    public static VersionedValue of(String value) 
    {
        return new VersionedValue(value, System.currentTimeMillis(), false);
    }

    public static VersionedValue tombstoneNow() 
    {
        return new VersionedValue("", System.currentTimeMillis(), true);
    }

    public boolean isNewerThan(VersionedValue other)
     {
        if (other == null)
        {
            return true;
        }
        return this.timestamp > other.timestamp;
    }
}

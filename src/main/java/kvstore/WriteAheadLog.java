package kvstore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.function.BiConsumer;

public class WriteAheadLog 
{

    private final Path logPath;
    private final BufferedWriter writer;

    public WriteAheadLog(String filePath) throws IOException 
    {
        this.logPath = Paths.get(filePath);
        if (!Files.exists(logPath)) 
        {
            if (logPath.toAbsolutePath().getParent() != null) 
            {
                Files.createDirectories(logPath.toAbsolutePath().getParent());
            }
            Files.createFile(logPath);
        }
        this.writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(logPath.toFile(), true), StandardCharsets.UTF_8)
        );
    }

    public synchronized void logSet(String key, VersionedValue v) throws IOException 
    {
        String line = String.join(",",
                "SET",
                escape(key),
                escape(v.value),
                Long.toString(v.timestamp),
                v.tombstone ? "1" : "0"
        );
        writer.write(line);
        writer.newLine();
        writer.flush(); 
    }

    public void replay(BiConsumer<String, VersionedValue> apply) throws IOException 
    {
        if (!Files.exists(logPath))
        {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(logPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) 
            {
                if (line.isBlank())
                {
                    continue;
                }
                String[] parts = splitEscaped(line);
                if (parts.length != 5 || !parts[0].equals("SET"))
                {
                    continue;
                }
                String key = unescape(parts[1]);
                String value = unescape(parts[2]);
                long ts = Long.parseLong(parts[3]);
                boolean tombstone = parts[4].equals("1");
                apply.accept(key, new VersionedValue(value, ts, tombstone));
            }
        }
    }

    public synchronized void close() throws IOException 
    {
        writer.close();
    }

    private String escape(String s) 
    {
        return s.replace("\\", "\\\\").replace(",", "\\,").replace("\n", "\\n");
    }

    private String unescape(String s) 
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) 
        {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) 
            {
                char next = s.charAt(i + 1);
                if (next == 'n')
                {
                    sb.append('\n'); i++;
                    continue;
                }
                if (next == ',' || next == '\\')
                {
                    sb.append(next); 
                    i++; 
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private String[] splitEscaped(String line) 
    {
        java.util.List<String> parts = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < line.length(); i++) 
        {
            char c = line.charAt(i);
            if (c == '\\' && i + 1 < line.length()) 
            {
                current.append(c).append(line.charAt(i + 1));
                i++;
            } 
            else if (c == ',') 
            {
                parts.add(current.toString());
                current = new StringBuilder();
            } 
            else 
            {
                current.append(c);
            }
        }
        parts.add(current.toString());
        return parts.toArray(new String[0]);
    }
}

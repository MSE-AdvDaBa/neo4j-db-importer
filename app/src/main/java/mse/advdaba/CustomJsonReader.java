package mse.advdaba;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.IOException;
import java.io.Reader;

public class CustomJsonReader extends JsonReader {
    // extend JsonReader but each string must be passed through sanitizeString
    public CustomJsonReader(Reader in) {
        super(in);
    }

    @Override
    public String nextString() throws IOException {
        return sanitizeString(super.nextString());
    }

    @Override
    public String nextName() throws IOException {
        return sanitizeString(super.nextName());
    }

    @Override
    public int nextInt() throws IOException {
        // if next value is not a number, convert it to a string and sanitize it
        if (peek() != JsonToken.NUMBER) {
            return Integer.parseInt(sanitizeString(nextString()));
        }
        return super.nextInt();
    }

    private String sanitizeString(String s) {
        String value = s.replaceAll("(NumberInt)\\(([0-9]+)(\\))", "$2");
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("'", "\\'");
    }

}

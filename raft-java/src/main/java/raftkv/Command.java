package raftkv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** A state-machine command carried inside a replicated log entry. */
public record Command(Type type, String key, String value) {

    public enum Type {
        NOOP(0), PUT(1), DELETE(2);

        private final int code;

        Type(int code) {
            this.code = code;
        }

        int code() {
            return code;
        }

        static Type fromCode(int code) throws IOException {
            for (Type t : values()) {
                if (t.code == code) {
                    return t;
                }
            }
            throw new IOException("unknown command type code " + code);
        }
    }

    private static final int MAX_FIELD_BYTES = 16 * 1024 * 1024;

    public Command {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
    }

    public static Command noop() {
        return new Command(Type.NOOP, "", "");
    }

    public static Command put(String key, String value) {
        return new Command(Type.PUT, key, value);
    }

    public static Command delete(String key) {
        return new Command(Type.DELETE, key, "");
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(type.code());
        writeString(out, key);
        writeString(out, value);
    }

    public static Command readFrom(DataInput in) throws IOException {
        Type t = Type.fromCode(in.readUnsignedByte());
        String k = readString(in);
        String v = readString(in);
        return new Command(t, k, v);
    }

    private static void writeString(DataOutput out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInput in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > MAX_FIELD_BYTES) {
            throw new IOException("invalid string length " + n);
        }
        byte[] b = new byte[n];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }
}

package raftkv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/** One replicated log entry. Indices are 1-based and contiguous. */
public record LogEntry(long term, long index, Command command) {

    public void writeTo(DataOutput out) throws IOException {
        out.writeLong(term);
        out.writeLong(index);
        command.writeTo(out);
    }

    public static LogEntry readFrom(DataInput in) throws IOException {
        long term = in.readLong();
        long index = in.readLong();
        Command command = Command.readFrom(in);
        return new LogEntry(term, index, command);
    }

    public byte[] encode() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(bos)) {
            writeTo(dos);
            dos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static LogEntry decode(byte[] payload) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
        LogEntry entry = readFrom(in);
        if (in.available() != 0) {
            throw new IOException("trailing bytes after log entry");
        }
        return entry;
    }
}

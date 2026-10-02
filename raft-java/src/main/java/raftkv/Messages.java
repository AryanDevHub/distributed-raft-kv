package raftkv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** RPC message types and their binary wire format (big-endian, JDK streams only). */
public final class Messages {

    private Messages() {
    }

    private static final int MAX_ENTRIES_PER_MESSAGE = 100_000;

    @FunctionalInterface
    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }

    private static byte[] toBytes(Writer writer) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(bos)) {
            writer.write(out);
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static DataInputStream stream(byte[] bytes) {
        return new DataInputStream(new ByteArrayInputStream(bytes));
    }

    public record RequestVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        public byte[] encode() {
            return toBytes(out -> {
                out.writeLong(term);
                out.writeUTF(candidateId);
                out.writeLong(lastLogIndex);
                out.writeLong(lastLogTerm);
            });
        }

        public static RequestVoteRequest decode(byte[] bytes) throws IOException {
            DataInputStream in = stream(bytes);
            return new RequestVoteRequest(in.readLong(), in.readUTF(), in.readLong(), in.readLong());
        }
    }

    public record RequestVoteResponse(long term, boolean voteGranted) {
        public byte[] encode() {
            return toBytes(out -> {
                out.writeLong(term);
                out.writeBoolean(voteGranted);
            });
        }

        public static RequestVoteResponse decode(byte[] bytes) throws IOException {
            DataInputStream in = stream(bytes);
            return new RequestVoteResponse(in.readLong(), in.readBoolean());
        }
    }

    public record AppendEntriesRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                       List<LogEntry> entries, long leaderCommit) {
        public byte[] encode() {
            return toBytes(out -> {
                out.writeLong(term);
                out.writeUTF(leaderId);
                out.writeLong(prevLogIndex);
                out.writeLong(prevLogTerm);
                out.writeLong(leaderCommit);
                out.writeInt(entries.size());
                for (LogEntry e : entries) {
                    e.writeTo(out);
                }
            });
        }

        public static AppendEntriesRequest decode(byte[] bytes) throws IOException {
            DataInputStream in = stream(bytes);
            long term = in.readLong();
            String leaderId = in.readUTF();
            long prevLogIndex = in.readLong();
            long prevLogTerm = in.readLong();
            long leaderCommit = in.readLong();
            int n = in.readInt();
            if (n < 0 || n > MAX_ENTRIES_PER_MESSAGE) {
                throw new IOException("invalid entry count " + n);
            }
            List<LogEntry> entries = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                entries.add(LogEntry.readFrom(in));
            }
            return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
        }
    }

    /**
     * @param matchIndex    on success: highest index known to match the leader's log
     * @param conflictIndex on failure: hint for the leader's next probe
     */
    public record AppendEntriesResponse(long term, boolean success, long matchIndex, long conflictIndex) {
        public byte[] encode() {
            return toBytes(out -> {
                out.writeLong(term);
                out.writeBoolean(success);
                out.writeLong(matchIndex);
                out.writeLong(conflictIndex);
            });
        }

        public static AppendEntriesResponse decode(byte[] bytes) throws IOException {
            DataInputStream in = stream(bytes);
            return new AppendEntriesResponse(in.readLong(), in.readBoolean(), in.readLong(), in.readLong());
        }
    }
}

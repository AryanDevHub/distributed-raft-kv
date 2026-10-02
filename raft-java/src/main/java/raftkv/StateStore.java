package raftkv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * Crash-safe persistence of Raft's hard state (currentTerm, votedFor).
 * Update protocol: write temp file -> fsync -> atomic rename over the live file -> fsync directory.
 * A crash leaves either the complete old state or the complete new state, never a mixture,
 * so a rebooted node can never vote twice in the same term.
 */
public final class StateStore {

    public record HardState(long currentTerm, String votedFor) {
    }

    private static final int MAGIC = 0x52414654; // "RAFT"

    private final Path dir;
    private final Path file;
    private final Path tmp;

    public StateStore(Path dir) throws IOException {
        this.dir = dir;
        Files.createDirectories(dir);
        this.file = dir.resolve("hardstate.bin");
        this.tmp = dir.resolve("hardstate.bin.tmp");
    }

    public HardState load() throws IOException {
        Files.deleteIfExists(tmp); // leftover from a crash before the rename; the live file is authoritative
        if (!Files.exists(file)) {
            return new HardState(0L, null);
        }
        byte[] raw = Files.readAllBytes(file);
        if (raw.length < 4 + 8 + 4 + 8) {
            throw new IOException("hard state file too short: " + file);
        }
        ByteBuffer buf = ByteBuffer.wrap(raw);
        if (buf.getInt() != MAGIC) {
            throw new IOException("hard state file has bad magic: " + file);
        }
        long term = buf.getLong();
        int voteLen = buf.getInt();
        if (voteLen < -1 || voteLen > raw.length - buf.position() - 8) {
            throw new IOException("hard state file has invalid vote length");
        }
        String votedFor = null;
        if (voteLen >= 0) {
            byte[] v = new byte[voteLen];
            buf.get(v);
            votedFor = new String(v, StandardCharsets.UTF_8);
        }
        int bodyLen = buf.position();
        long storedCrc = buf.getLong();
        CRC32 crc = new CRC32();
        crc.update(raw, 0, bodyLen);
        if (crc.getValue() != storedCrc) {
            throw new IOException("hard state file checksum mismatch: " + file);
        }
        return new HardState(term, votedFor);
    }

    public void save(long term, String votedFor) throws IOException {
        byte[] vote = votedFor == null ? new byte[0] : votedFor.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + 8 + 4 + vote.length + 8);
        buf.putInt(MAGIC);
        buf.putLong(term);
        buf.putInt(votedFor == null ? -1 : vote.length);
        buf.put(vote);
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, buf.position());
        buf.putLong(crc.getValue());
        buf.flip();

        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        }
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE);
        syncDirectory(dir);
    }

    /** Best-effort fsync of a directory so a rename/create is durable (unsupported on some platforms). */
    static void syncDirectory(Path directory) {
        try (FileChannel ch = FileChannel.open(directory, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // Windows cannot open directories; the rename itself is still atomic.
        }
    }
}

package raftkv;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * Binary write-ahead log. Frame layout on disk (big-endian):
 * <pre>
 *   4 bytes  magic   0xDEADCAFE
 *   4 bytes  payload length N
 *   N bytes  payload (serialized LogEntry)
 *   4 bytes  CRC32 of the payload bytes
 * </pre>
 * Every append is followed by FileChannel.force(true). {@link #recoverAll()} must be called once
 * before any append; it validates frames from offset 0 and truncates a torn/corrupt tail.
 */
public final class BinaryWAL implements Closeable {

    public static final int MAGIC = 0xDEADCAFE;

    private static final int HEADER_BYTES = 8;
    private static final int CRC_BYTES = 4;
    private static final int MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;

    private final FileChannel channel;
    private final ReentrantLock lock = new ReentrantLock();
    /** frameOffsets.get(i) is the file offset of the frame holding the entry with index i + 1. */
    private final List<Long> frameOffsets = new ArrayList<>();
    private long writeOffset;
    private boolean recovered;

    public BinaryWAL(Path dir) throws IOException {
        Files.createDirectories(dir);
        Path file = dir.resolve("raft.wal");
        this.channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        StateStore.syncDirectory(dir);
    }

    /** Reads the log from byte 0, truncating at the last verified frame, and returns the valid entries. */
    public List<LogEntry> recoverAll() throws IOException {
        lock.lock();
        try {
            List<LogEntry> entries = new ArrayList<>();
            frameOffsets.clear();
            long size = channel.size();
            long pos = 0;
            while (size - pos >= HEADER_BYTES + CRC_BYTES) {
                ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
                readFully(header, pos);
                header.flip();
                int magic = header.getInt();
                int length = header.getInt();
                if (magic != MAGIC || length < 0 || length > MAX_PAYLOAD_BYTES) {
                    break;
                }
                long frameBytes = (long) HEADER_BYTES + length + CRC_BYTES;
                if (size - pos < frameBytes) {
                    break; // partial write at the tail
                }
                ByteBuffer body = ByteBuffer.allocate(length + CRC_BYTES);
                readFully(body, pos + HEADER_BYTES);
                body.flip();
                byte[] payload = new byte[length];
                body.get(payload);
                long storedCrc = body.getInt() & 0xFFFFFFFFL;
                CRC32 crc = new CRC32();
                crc.update(payload);
                if (crc.getValue() != storedCrc) {
                    break;
                }
                LogEntry entry;
                try {
                    entry = LogEntry.decode(payload);
                } catch (IOException | RuntimeException e) {
                    break;
                }
                if (entry.index() != entries.size() + 1L) {
                    break;
                }
                entries.add(entry);
                frameOffsets.add(pos);
                pos += frameBytes;
            }
            if (pos < size) {
                System.err.printf("WAL: discarding %d trailing invalid bytes at offset %d%n", size - pos, pos);
                channel.truncate(pos);
                channel.force(true);
            }
            writeOffset = pos;
            recovered = true;
            return entries;
        } finally {
            lock.unlock();
        }
    }

    /** Appends the entries (indices must continue the log contiguously) and fsyncs once. */
    public void appendAll(List<LogEntry> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }
        lock.lock();
        try {
            requireRecovered();
            long expected = frameOffsets.size() + 1L;
            for (LogEntry e : entries) {
                if (e.index() != expected++) {
                    throw new IllegalStateException("non-contiguous append: got index " + e.index()
                            + ", expected " + (expected - 1));
                }
            }
            long startOffset = writeOffset;
            long offset = writeOffset;
            List<Long> newOffsets = new ArrayList<>(entries.size());
            try {
                for (LogEntry e : entries) {
                    ByteBuffer frame = encodeFrame(e);
                    int frameLength = frame.remaining();
                    writeFully(frame, offset);
                    newOffsets.add(offset);
                    offset += frameLength;
                }
                channel.force(true);
            } catch (IOException ex) {
                try {
                    channel.truncate(startOffset);
                } catch (IOException ignored) {
                    // the caller treats an append failure as fatal
                }
                throw ex;
            }
            frameOffsets.addAll(newOffsets);
            writeOffset = offset;
        } finally {
            lock.unlock();
        }
    }

    public void append(LogEntry entry) throws IOException {
        appendAll(List.of(entry));
    }

    /** Removes every entry with index >= fromIndex and fsyncs. */
    public void truncateSuffix(long fromIndex) throws IOException {
        if (fromIndex < 1) {
            throw new IllegalArgumentException("fromIndex must be >= 1");
        }
        lock.lock();
        try {
            requireRecovered();
            if (fromIndex > frameOffsets.size()) {
                return;
            }
            int from = (int) (fromIndex - 1);
            long offset = frameOffsets.get(from);
            channel.truncate(offset);
            channel.force(true);
            frameOffsets.subList(from, frameOffsets.size()).clear();
            writeOffset = offset;
        } finally {
            lock.unlock();
        }
    }

    public long lastIndex() {
        lock.lock();
        try {
            return frameOffsets.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            channel.close();
        } finally {
            lock.unlock();
        }
    }

    private void requireRecovered() {
        if (!recovered) {
            throw new IllegalStateException("recoverAll() must be called before writing");
        }
    }

    private static ByteBuffer encodeFrame(LogEntry entry) {
        byte[] payload = entry.encode();
        CRC32 crc = new CRC32();
        crc.update(payload);
        ByteBuffer frame = ByteBuffer.allocate(HEADER_BYTES + payload.length + CRC_BYTES);
        frame.putInt(MAGIC);
        frame.putInt(payload.length);
        frame.put(payload);
        frame.putInt((int) crc.getValue());
        frame.flip();
        return frame;
    }

    private void readFully(ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = channel.read(buf, pos);
            if (n < 0) {
                throw new EOFException("unexpected end of WAL at offset " + pos);
            }
            pos += n;
        }
    }

    private void writeFully(ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            pos += channel.write(buf, pos);
        }
    }
}

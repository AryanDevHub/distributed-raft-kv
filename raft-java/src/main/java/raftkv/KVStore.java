package raftkv;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Thread-safe in-memory key-value state machine. Knows nothing about Raft, disks, or the network. */
public final class KVStore {

    private final Map<String, String> data = new HashMap<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** Applies one committed command. Must be called sequentially in log order. */
    public void apply(Command cmd) {
        switch (cmd.type()) {
            case PUT -> {
                rwLock.writeLock().lock();
                try {
                    data.put(cmd.key(), cmd.value());
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
            case DELETE -> {
                rwLock.writeLock().lock();
                try {
                    data.remove(cmd.key());
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
            case NOOP -> {
                // leader-election barrier entries change nothing
            }
        }
    }

    public Optional<String> get(String key) {
        rwLock.readLock().lock();
        try {
            return Optional.ofNullable(data.get(key));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int size() {
        rwLock.readLock().lock();
        try {
            return data.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }
}

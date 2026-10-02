package raftkv;

import java.util.List;

/**
 * Side effects requested by {@link RaftCore}. The core never performs I/O itself;
 * the runtime executes these in list order (hard-state persistence is always first).
 */
public sealed interface Action {

    /** Durably persist currentTerm and votedFor BEFORE any message is sent. */
    record PersistHardState(long term, String votedFor) implements Action {}

    /** Append entries to the tail of the write-ahead log and fsync. */
    record AppendToLog(List<LogEntry> entries) implements Action {}

    /** Delete every log entry with index >= index. */
    record TruncateLogFrom(long index) implements Action {}

    record SendRequestVote(String peerId, Messages.RequestVoteRequest request) implements Action {}

    record SendAppendEntries(String peerId, Messages.AppendEntriesRequest request) implements Action {}

    /** Newly committed entries, in order, to be applied to the key-value store. */
    record ApplyEntries(List<LogEntry> entries) implements Action {}

    /** (Re)arm the randomized election timer. */
    record ResetElectionTimer() implements Action {}

    /** Disarm the election timer (leaders do not time out). */
    record CancelElectionTimer() implements Action {}
}

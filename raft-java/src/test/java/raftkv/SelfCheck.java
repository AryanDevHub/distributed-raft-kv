package raftkv;

import raftkv.Messages.AppendEntriesRequest;
import raftkv.Messages.AppendEntriesResponse;
import raftkv.Messages.RequestVoteRequest;
import raftkv.Messages.RequestVoteResponse;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Dependency-free checks for the properties the design depends on. Run with:
 * java -cp out:out-test raftkv.SelfCheck     (exits non-zero on the first failure)
 */
public final class SelfCheck {

    private static int checks;

    private static void check(boolean condition, String what) {
        checks++;
        if (!condition) {
            throw new AssertionError("FAILED: " + what);
        }
        System.out.println("  ok  " + what);
    }

    private static LogEntry entry(long term, long index) {
        return new LogEntry(term, index, Command.put("k" + index, "v" + index));
    }

    public static void main(String[] args) throws Exception {
        walRecovery();
        hardState();
        quorumFormula();
        voteLogUpToDateRule();
        oldTermEntriesCommitOnlyIndirectly();
        conflictingEntriesAreOverwritten();
        System.out.println("All " + checks + " checks passed.");
    }

    static void walRecovery() throws IOException {
        System.out.println("BinaryWAL");
        Path dir = Files.createTempDirectory("wal-check");
        Path file = dir.resolve("raft.wal");
        long validSize;
        try (BinaryWAL wal = new BinaryWAL(dir)) {
            check(wal.recoverAll().isEmpty(), "fresh log recovers empty");
            wal.appendAll(List.of(entry(1, 1), entry(1, 2)));
            wal.append(entry(2, 3));
        }
        validSize = Files.size(file);

        // Torn write: a header promising 100 payload bytes, but only 3 arrive before "power loss".
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.writeInt(BinaryWAL.MAGIC);
            raf.writeInt(100);
            raf.write(new byte[]{1, 2, 3});
        }
        try (BinaryWAL wal = new BinaryWAL(dir)) {
            List<LogEntry> recovered = wal.recoverAll();
            check(recovered.size() == 3, "partial tail frame ignored, 3 valid entries recovered");
            check(Files.size(file) == validSize, "log truncated back to last valid offset");
            wal.append(entry(2, 4));
            wal.truncateSuffix(4);
            check(wal.lastIndex() == 3 && Files.size(file) == validSize, "truncateSuffix removes entries on disk");
        }

        // Bit rot in the last frame's payload: CRC32 mismatch must drop that frame.
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length() - 6);
            int b = raf.read();
            raf.seek(raf.length() - 6);
            raf.write(b ^ 0xFF);
        }
        try (BinaryWAL wal = new BinaryWAL(dir)) {
            check(wal.recoverAll().size() == 2, "frame with bad CRC32 dropped, earlier entries kept");
        }
    }

    static void hardState() throws IOException {
        System.out.println("StateStore");
        Path dir = Files.createTempDirectory("state-check");
        StateStore store = new StateStore(dir);
        check(store.load().currentTerm() == 0 && store.load().votedFor() == null, "missing file loads as term 0, no vote");
        store.save(7, "node2");
        store.save(8, null);
        Files.writeString(dir.resolve("hardstate.bin.tmp"), "half-written garbage");
        StateStore.HardState loaded = new StateStore(dir).load();
        check(loaded.currentTerm() == 8 && loaded.votedFor() == null, "latest atomic update wins; stray temp file ignored");
        store.save(9, "node3");
        check(new StateStore(dir).load().votedFor().equals("node3"), "votedFor round-trips");
    }

    static void quorumFormula() {
        System.out.println("Quorum");
        check(new RaftCore("a", List.of("b", "c"), 0, null, List.of()).quorum() == 2, "3-node cluster needs 2");
        check(new RaftCore("a", List.of("b", "c", "d", "e"), 0, null, List.of()).quorum() == 3, "5-node cluster needs 3");
        check(new RaftCore("a", List.of("b", "c", "d"), 0, null, List.of()).quorum() == 3, "4-node cluster needs 3");
        check(new RaftCore("a", List.of(), 0, null, List.of()).quorum() == 1, "single node needs 1");
    }

    static void voteLogUpToDateRule() {
        System.out.println("§5.4.1 vote safety");
        RaftCore core = new RaftCore("n1", List.of("n2", "n3"), 2, null, List.of(entry(2, 1)));
        RaftCore.Outcome<RequestVoteResponse> staleTerm = core.handleRequestVote(new RequestVoteRequest(3, "n2", 5, 1));
        check(!staleTerm.value().voteGranted(), "denied: candidate's last log term is lower despite a longer log");
        check(staleTerm.actions().get(0) instanceof Action.PersistHardState, "term bump persisted first");

        RaftCore.Outcome<RequestVoteResponse> shorter = core.handleRequestVote(new RequestVoteRequest(3, "n2", 0, 2));
        check(!shorter.value().voteGranted(), "denied: same last term but shorter log");

        RaftCore.Outcome<RequestVoteResponse> ok = core.handleRequestVote(new RequestVoteRequest(3, "n2", 1, 2));
        check(ok.value().voteGranted(), "granted: same last term, log at least as long");
        check(ok.actions().get(0) instanceof Action.PersistHardState p && "n2".equals(p.votedFor()),
                "vote persisted before the reply is sent");

        RaftCore.Outcome<RequestVoteResponse> second = core.handleRequestVote(new RequestVoteRequest(3, "n3", 9, 9));
        check(!second.value().voteGranted(), "denied: already voted for n2 in term 3");
        RaftCore.Outcome<RequestVoteResponse> repeat = core.handleRequestVote(new RequestVoteRequest(3, "n2", 1, 2));
        check(repeat.value().voteGranted(), "same candidate retrying is granted again (idempotent)");
    }

    static void oldTermEntriesCommitOnlyIndirectly() {
        System.out.println("§5.4.2 commit safety");
        RaftCore leader = new RaftCore("n1", List.of("n2", "n3"), 2, null, List.of(entry(1, 1)));
        leader.onElectionTimeout();
        List<Action> won = leader.handleRequestVoteResponse("n2", new RequestVoteResponse(3, true));
        check(leader.state() == RaftCore.State.LEADER && leader.currentTerm() == 3, "became leader in term 3");
        check(leader.lastLogIndex() == 2, "leader appended a term-3 no-op after the term-1 entry");

        AppendEntriesRequest toN2 = won.stream()
                .filter(a -> a instanceof Action.SendAppendEntries s && s.peerId().equals("n2"))
                .map(a -> ((Action.SendAppendEntries) a).request()).findFirst().orElseThrow();

        // n2 acknowledges ONLY the old-term entry (index 1): leader + n2 = 2 replicas = a quorum by count.
        leader.handleAppendEntriesResponse("n2", toN2, new AppendEntriesResponse(3, true, 1, 0));
        check(leader.commitIndex() == 0, "old-term entry NOT committed by counting replicas");

        // Once the current-term entry (index 2) reaches a quorum, both commit.
        List<Action> actions = leader.handleAppendEntriesResponse("n2", toN2, new AppendEntriesResponse(3, true, 2, 0));
        check(leader.commitIndex() == 2, "current-term entry reached quorum: commitIndex = 2");
        Action.ApplyEntries applied = actions.stream().filter(a -> a instanceof Action.ApplyEntries)
                .map(a -> (Action.ApplyEntries) a).findFirst().orElseThrow();
        check(applied.entries().size() == 2 && applied.entries().get(0).term() == 1,
                "old-term entry committed indirectly, in order");
    }

    static void conflictingEntriesAreOverwritten() {
        System.out.println("Conflict resolution");
        RaftCore follower = new RaftCore("n2", List.of("n1", "n3"), 1, null,
                List.of(entry(1, 1), entry(1, 2), entry(1, 3)));
        AppendEntriesRequest req = new AppendEntriesRequest(2, "n1", 1, 1, List.of(entry(2, 2)), 0);
        RaftCore.Outcome<AppendEntriesResponse> out = follower.handleAppendEntries(req);
        check(out.value().success() && out.value().matchIndex() == 2, "accepted with matchIndex 2");
        check(follower.lastLogIndex() == 2, "uncommitted entries 2 (term 1) and 3 replaced by leader's entry");
        int truncate = -1;
        int append = -1;
        for (int i = 0; i < out.actions().size(); i++) {
            if (out.actions().get(i) instanceof Action.TruncateLogFrom t && t.index() == 2) {
                truncate = i;
            }
            if (out.actions().get(i) instanceof Action.AppendToLog) {
                append = i;
            }
        }
        check(truncate >= 0 && append > truncate, "disk truncate is ordered before the append");

        AppendEntriesRequest gap = new AppendEntriesRequest(2, "n1", 7, 2, List.of(), 0);
        check(!follower.handleAppendEntries(gap).value().success(), "rejected when prevLogIndex would leave a gap");
        AppendEntriesRequest wrongTerm = new AppendEntriesRequest(2, "n1", 2, 1, List.of(), 0);
        check(!follower.handleAppendEntries(wrongTerm).value().success(), "rejected when prevLogTerm mismatches");
        AppendEntriesRequest stale = new AppendEntriesRequest(1, "n3", 0, 0, List.of(), 0);
        check(!follower.handleAppendEntries(stale).value().success(), "rejected when sender's term is stale");
    }
}

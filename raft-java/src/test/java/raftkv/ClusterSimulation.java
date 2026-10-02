package raftkv;

import raftkv.Messages.AppendEntriesRequest;
import raftkv.Messages.AppendEntriesResponse;
import raftkv.Messages.RequestVoteRequest;
import raftkv.Messages.RequestVoteResponse;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Deterministic, single-threaded simulation of whole Raft clusters. Several {@link RaftCore} instances run
 * against a virtual clock and a fault-injecting network that drops, delays, duplicates and reorders messages,
 * partitions the cluster, and crashes and restarts nodes (a restart keeps only what was persisted).
 * After every step the Raft safety properties are checked; after a chaos phase the network is healed and
 * liveness is checked too. A failing run is reproducible from its scenario and seed.
 *
 * Usage: java -cp out:out-test raftkv.ClusterSimulation [runsPerScenario=100] [firstSeed=1]
 */
public final class ClusterSimulation {

    record Scenario(String name, int nodes, int chaosMs, double drop, double dup,
                    int crashEveryMs, int partitionEveryMs, int proposeEveryMs, int maxDelayMs) {
    }

    static final class Violation extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Violation(String message) {
            super(message);
        }
    }

    static final class SimNode {
        final String id;
        boolean up = true;
        int incarnation;
        RaftCore core;
        // simulated disk: survives crashes
        long diskTerm;
        String diskVotedFor;
        final List<LogEntry> diskLog = new ArrayList<>();
        // volatile state: lost on crash
        long electionEpoch;
        final List<LogEntry> applied = new ArrayList<>();
        long lastTerm;
        long lastCommit;
        long leaderTermRecorded = -1;

        SimNode(String id) {
            this.id = id;
        }
    }

    private record Event(long time, long seq, String desc, Runnable action) {
    }

    static final class Run {
        final Scenario sc;
        final long seed;
        final Random rnd;
        final List<SimNode> nodes = new ArrayList<>();
        final Map<String, SimNode> byId = new HashMap<>();
        final Map<String, Integer> group = new HashMap<>();
        final PriorityQueue<Event> queue = new PriorityQueue<>(
                Comparator.comparingLong(Event::time).thenComparingLong(Event::seq));
        final ArrayDeque<String> trace = new ArrayDeque<>();
        final Map<Long, String> leaderByTerm = new HashMap<>();
        final Map<Long, LogEntry> committed = new HashMap<>();
        /** Term in which each index was first committed (the first node to apply it is the committing leader). */
        final Map<Long, Long> commitTerm = new HashMap<>();
        final List<String> finalKeys = new ArrayList<>();

        long now;
        long seq;
        long events;
        double drop;
        double dup;
        boolean chaos = true;
        int proposals;
        long maxCommitted;
        int crashes;
        int partitions;
        int elections;

        Run(Scenario sc, long seed) {
            this.sc = sc;
            this.seed = seed;
            this.rnd = new Random(seed);
            this.drop = sc.drop();
            this.dup = sc.dup();
            for (int i = 1; i <= sc.nodes(); i++) {
                SimNode n = new SimNode("n" + i);
                nodes.add(n);
                byId.put(n.id, n);
                group.put(n.id, 0);
            }
        }

        // ------------------------------------------------------------ driver

        void execute() {
            for (SimNode n : nodes) {
                n.core = new RaftCore(n.id, peersOf(n), 0, null, List.of());
                resetElection(n);
                scheduleHeartbeat(n, rnd.nextInt(50));
            }
            if (sc.crashEveryMs() > 0) {
                scheduleCrash();
            }
            if (sc.partitionEveryMs() > 0) {
                schedulePartition();
            }
            scheduleProposal();
            schedule(sc.chaosMs(), "HEAL", this::heal);
            schedule(sc.chaosMs() + 5_000L, "FINAL-PROPOSALS", this::finalProposals);

            long end = sc.chaosMs() + 10_000L;
            while (!queue.isEmpty() && queue.peek().time() <= end) {
                Event e = queue.poll();
                now = e.time();
                trace.addLast(now + "ms " + e.desc());
                if (trace.size() > 60) {
                    trace.removeFirst();
                }
                e.action().run();
                events++;
                if (events % 25 == 0) {
                    checkLogMatching();
                }
                if (events > 5_000_000L) {
                    throw new Violation("simulation did not terminate");
                }
            }
            finalChecks();
        }

        void schedule(long delay, String desc, Runnable r) {
            queue.add(new Event(now + delay, seq++, desc, r));
        }

        List<String> peersOf(SimNode n) {
            List<String> peers = new ArrayList<>();
            for (SimNode o : nodes) {
                if (o != n) {
                    peers.add(o.id);
                }
            }
            return peers;
        }

        // ------------------------------------------------------------ timers

        void resetElection(SimNode n) {
            final long epoch = ++n.electionEpoch;
            int timeout = 150 + rnd.nextInt(201); // 150..350 ms, as in the real server
            schedule(timeout, "election-timer " + n.id, () -> {
                if (!n.up || n.electionEpoch != epoch) {
                    return;
                }
                exec(n, n.core.onElectionTimeout());
            });
        }

        void scheduleHeartbeat(SimNode n, long delay) {
            schedule(delay, "heartbeat " + n.id, () -> {
                if (n.up) {
                    exec(n, n.core.onHeartbeatTick());
                }
                scheduleHeartbeat(n, 50);
            });
        }

        // ------------------------------------------------------------ chaos

        void scheduleCrash() {
            schedule(1 + rnd.nextInt(2 * sc.crashEveryMs()), "chaos-crash", () -> {
                if (!chaos) {
                    return;
                }
                SimNode n = nodes.get(rnd.nextInt(nodes.size()));
                if (n.up) {
                    crash(n);
                    schedule(50 + rnd.nextInt(2000), "restart " + n.id, () -> restart(n));
                }
                scheduleCrash();
            });
        }

        void schedulePartition() {
            schedule(1 + rnd.nextInt(2 * sc.partitionEveryMs()), "chaos-partition", () -> {
                if (!chaos) {
                    return;
                }
                switch (rnd.nextInt(3)) {
                    case 0 -> setAllGroups(0);
                    case 1 -> {
                        setAllGroups(0);
                        group.put(nodes.get(rnd.nextInt(nodes.size())).id, 1);
                    }
                    default -> {
                        for (SimNode n : nodes) {
                            group.put(n.id, rnd.nextInt(2));
                        }
                    }
                }
                partitions++;
                trace.addLast(now + "ms   partition groups=" + group);
                schedulePartition();
            });
        }

        void setAllGroups(int g) {
            for (SimNode n : nodes) {
                group.put(n.id, g);
            }
        }

        void scheduleProposal() {
            schedule(1 + rnd.nextInt(2 * sc.proposeEveryMs()), "client-propose", () -> {
                if (!chaos) {
                    return;
                }
                propose(pickProposer(), Command.put("k" + proposals, "v" + proposals));
                scheduleProposal();
            });
        }

        SimNode pickProposer() {
            List<SimNode> leaders = new ArrayList<>();
            List<SimNode> up = new ArrayList<>();
            for (SimNode n : nodes) {
                if (n.up) {
                    up.add(n);
                    if (n.core.state() == RaftCore.State.LEADER) {
                        leaders.add(n);
                    }
                }
            }
            List<SimNode> pool = leaders.isEmpty() ? up : leaders;
            return pool.isEmpty() ? null : pool.get(rnd.nextInt(pool.size()));
        }

        void propose(SimNode n, Command command) {
            if (n == null) {
                return;
            }
            RaftCore.Outcome<RaftCore.ProposeResult> outcome = n.core.propose(command);
            if (outcome.value().accepted()) {
                proposals++;
                trace.addLast(now + "ms   " + n.id + " accepted " + command.key() + " at index "
                        + outcome.value().index() + " term " + outcome.value().term());
                exec(n, outcome.actions());
            }
        }

        void crash(SimNode n) {
            if (!n.up) {
                return;
            }
            n.up = false;
            n.incarnation++;
            n.electionEpoch++;
            crashes++;
            trace.addLast(now + "ms   CRASH " + n.id);
        }

        void restart(SimNode n) {
            if (n.up) {
                return;
            }
            n.up = true;
            n.core = new RaftCore(n.id, peersOf(n), n.diskTerm, n.diskVotedFor, new ArrayList<>(n.diskLog));
            if (n.core.currentTerm() < n.lastTerm) {
                throw new Violation(n.id + " lost its term across a restart: " + n.lastTerm + " -> "
                        + n.core.currentTerm());
            }
            n.lastTerm = n.core.currentTerm();
            n.lastCommit = 0;
            n.applied.clear();
            n.leaderTermRecorded = -1;
            trace.addLast(now + "ms   RESTART " + n.id + " term=" + n.diskTerm + " voted=" + n.diskVotedFor
                    + " log=" + n.diskLog.size());
            resetElection(n);
        }

        void heal() {
            chaos = false;
            drop = 0;
            dup = 0;
            setAllGroups(0);
            for (SimNode n : nodes) {
                restart(n);
            }
        }

        void finalProposals() {
            SimNode leader = null;
            for (SimNode n : nodes) {
                if (n.up && n.core.state() == RaftCore.State.LEADER
                        && (leader == null || n.core.currentTerm() > leader.core.currentTerm())) {
                    leader = n;
                }
            }
            if (leader == null) {
                throw new Violation("liveness: no leader 5 seconds after the network healed");
            }
            for (int i = 0; i < 3; i++) {
                String key = "final" + i;
                finalKeys.add(key);
                propose(leader, Command.put(key, "x"));
            }
        }

        // ------------------------------------------------------------ network

        boolean reachable(String a, String b) {
            return group.get(a).equals(group.get(b));
        }

        void deliver(String from, String to, String desc, Runnable atDestination) {
            if (!reachable(from, to) || rnd.nextDouble() < drop) {
                return;
            }
            int copies = rnd.nextDouble() < dup ? 2 : 1;
            for (int i = 0; i < copies; i++) {
                int delay = rnd.nextInt(100) < 5 ? 20 + rnd.nextInt(sc.maxDelayMs()) : 1 + rnd.nextInt(20);
                schedule(delay, desc, () -> {
                    if (reachable(from, to)) {
                        atDestination.run();
                    }
                });
            }
        }

        void sendVote(SimNode from, String to, RequestVoteRequest req) {
            final int incarnation = from.incarnation;
            deliver(from.id, to, "RequestVote " + from.id + "->" + to + " term " + req.term(), () -> {
                SimNode dst = byId.get(to);
                if (!dst.up) {
                    return;
                }
                RaftCore.Outcome<RequestVoteResponse> out = dst.core.handleRequestVote(req);
                exec(dst, out.actions());
                assertDurable(dst, "RequestVote reply");
                RequestVoteResponse resp = out.value();
                deliver(to, from.id, "VoteReply " + to + "->" + from.id + " term " + resp.term()
                        + (resp.voteGranted() ? " GRANTED" : " denied"), () -> {
                    if (!from.up || from.incarnation != incarnation) {
                        return; // the process that asked is gone
                    }
                    exec(from, from.core.handleRequestVoteResponse(to, resp));
                });
            });
        }

        void sendAppend(SimNode from, String to, AppendEntriesRequest req) {
            final int incarnation = from.incarnation;
            deliver(from.id, to, "AppendEntries " + from.id + "->" + to + " term " + req.term() + " prev "
                    + req.prevLogIndex() + " n=" + req.entries().size() + " commit " + req.leaderCommit(), () -> {
                SimNode dst = byId.get(to);
                if (!dst.up) {
                    return;
                }
                RaftCore.Outcome<AppendEntriesResponse> out = dst.core.handleAppendEntries(req);
                exec(dst, out.actions());
                assertDurable(dst, "AppendEntries reply");
                AppendEntriesResponse resp = out.value();
                deliver(to, from.id, "AppendReply " + to + "->" + from.id + " term " + resp.term()
                        + (resp.success() ? " ok match " + resp.matchIndex() : " reject hint "
                        + resp.conflictIndex()), () -> {
                    if (!from.up || from.incarnation != incarnation) {
                        return;
                    }
                    exec(from, from.core.handleAppendEntriesResponse(to, req, resp));
                });
            });
        }

        // ------------------------------------------------------------ executing core actions

        void exec(SimNode n, List<Action> actions) {
            for (Action a : actions) {
                if (a instanceof Action.PersistHardState p) {
                    n.diskTerm = p.term();
                    n.diskVotedFor = p.votedFor();
                } else if (a instanceof Action.AppendToLog al) {
                    for (LogEntry e : al.entries()) {
                        if (e.index() != n.diskLog.size() + 1L) {
                            throw new Violation(n.id + " appended index " + e.index() + " after "
                                    + n.diskLog.size() + " entries");
                        }
                        n.diskLog.add(e);
                    }
                } else if (a instanceof Action.TruncateLogFrom t) {
                    for (long i = t.index(); i <= n.diskLog.size(); i++) {
                        LogEntry c = committed.get(i);
                        if (c != null && c.equals(n.diskLog.get((int) i - 1))) {
                            throw new Violation(n.id + " truncated committed entry " + i);
                        }
                    }
                    while (n.diskLog.size() >= t.index()) {
                        n.diskLog.remove(n.diskLog.size() - 1);
                    }
                } else if (a instanceof Action.SendRequestVote s) {
                    assertDurable(n, "RequestVote send");
                    sendVote(n, s.peerId(), s.request());
                } else if (a instanceof Action.SendAppendEntries s) {
                    assertDurable(n, "AppendEntries send");
                    sendAppend(n, s.peerId(), s.request());
                } else if (a instanceof Action.ApplyEntries ap) {
                    apply(n, ap.entries());
                } else if (a instanceof Action.ResetElectionTimer) {
                    resetElection(n);
                } else if (a instanceof Action.CancelElectionTimer) {
                    n.electionEpoch++;
                } else {
                    throw new Violation("unknown action " + a);
                }
            }
            afterExec(n);
        }

        void apply(SimNode n, List<LogEntry> entries) {
            for (LogEntry e : entries) {
                if (e.index() != n.applied.size() + 1L) {
                    throw new Violation(n.id + " applied index " + e.index() + " but expected "
                            + (n.applied.size() + 1));
                }
                if (e.index() > n.core.commitIndex()) {
                    throw new Violation(n.id + " applied index " + e.index() + " beyond commitIndex "
                            + n.core.commitIndex());
                }
                if (e.index() > n.diskLog.size() || !n.diskLog.get((int) e.index() - 1).equals(e)) {
                    throw new Violation(n.id + " applied entry " + e.index() + " that is not in its durable log");
                }
                n.applied.add(e);
                LogEntry prior = committed.putIfAbsent(e.index(), e);
                commitTerm.putIfAbsent(e.index(), n.core.currentTerm());
                if (prior != null && !prior.equals(e)) {
                    throw new Violation("state machine safety: index " + e.index() + " was applied as " + prior
                            + " elsewhere but as " + e + " on " + n.id);
                }
                maxCommitted = Math.max(maxCommitted, e.index());
            }
        }

        // ------------------------------------------------------------ invariants

        /** Nothing may leave a node (and no reply may be sent) before its state is on "disk". */
        void assertDurable(SimNode n, String context) {
            RaftCore c = n.core;
            if (n.diskTerm != c.currentTerm() || !Objects.equals(n.diskVotedFor, c.votedFor())) {
                throw new Violation("durability: " + n.id + " sent a " + context + " before persisting term/vote "
                        + "(memory term=" + c.currentTerm() + " vote=" + c.votedFor()
                        + ", disk term=" + n.diskTerm + " vote=" + n.diskVotedFor + ")");
            }
            if (n.diskLog.size() != c.lastLogIndex()) {
                throw new Violation("durability: " + n.id + " sent a " + context + " with " + c.lastLogIndex()
                        + " log entries in memory but " + n.diskLog.size() + " on disk");
            }
        }

        void afterExec(SimNode n) {
            RaftCore c = n.core;
            if (c.currentTerm() < n.lastTerm) {
                throw new Violation(n.id + " term went backwards: " + n.lastTerm + " -> " + c.currentTerm());
            }
            n.lastTerm = c.currentTerm();
            if (c.commitIndex() < n.lastCommit) {
                throw new Violation(n.id + " commitIndex went backwards: " + n.lastCommit + " -> " + c.commitIndex());
            }
            n.lastCommit = c.commitIndex();
            if (c.commitIndex() > c.lastLogIndex()) {
                throw new Violation(n.id + " commitIndex " + c.commitIndex() + " beyond its log " + c.lastLogIndex());
            }
            if (c.state() == RaftCore.State.LEADER && n.leaderTermRecorded != c.currentTerm()) {
                long term = c.currentTerm();
                String prior = leaderByTerm.putIfAbsent(term, n.id);
                if (prior != null && !prior.equals(n.id)) {
                    throw new Violation("election safety: two leaders in term " + term + ": " + prior + " and " + n.id);
                }
                n.leaderTermRecorded = term;
                elections++;
                for (long i = 1; i <= maxCommitted; i++) {
                    if (commitTerm.get(i) >= term) {
                        continue; // committed in this term or later: a stale-term leader need not have it
                    }
                    if (i > n.diskLog.size() || !n.diskLog.get((int) i - 1).equals(committed.get(i))) {
                        throw new Violation("leader completeness: " + n.id + " became leader in term " + term
                                + " without entry " + i + ", committed in term " + commitTerm.get(i));
                    }
                }
            }
        }

        /** Log Matching: if two logs agree on (index, term), they are identical up to that index. */
        void checkLogMatching() {
            for (int a = 0; a < nodes.size(); a++) {
                for (int b = a + 1; b < nodes.size(); b++) {
                    List<LogEntry> la = nodes.get(a).diskLog;
                    List<LogEntry> lb = nodes.get(b).diskLog;
                    int common = Math.min(la.size(), lb.size());
                    for (int i = common - 1; i >= 0; i--) {
                        if (la.get(i).term() == lb.get(i).term()) {
                            for (int j = 0; j <= i; j++) {
                                if (!la.get(j).equals(lb.get(j))) {
                                    throw new Violation("log matching: " + nodes.get(a).id + " and "
                                            + nodes.get(b).id + " agree on term at index " + (i + 1)
                                            + " but differ at index " + (j + 1));
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }

        /** After healing, the cluster must have converged and still make progress. */
        void finalChecks() {
            checkLogMatching();
            List<SimNode> leaders = new ArrayList<>();
            for (SimNode n : nodes) {
                if (!n.up) {
                    throw new Violation("liveness: " + n.id + " is still down after the heal");
                }
                if (n.core.state() == RaftCore.State.LEADER) {
                    leaders.add(n);
                }
            }
            if (leaders.size() != 1) {
                throw new Violation("liveness: expected exactly one leader after healing, found " + leaders.size());
            }
            SimNode ref = nodes.get(0);
            for (SimNode n : nodes) {
                if (!n.diskLog.equals(ref.diskLog)) {
                    throw new Violation("liveness: logs did not converge (" + n.id + " has " + n.diskLog.size()
                            + " entries, " + ref.id + " has " + ref.diskLog.size() + ")");
                }
                if (n.core.commitIndex() != n.core.lastLogIndex()) {
                    throw new Violation("liveness: " + n.id + " has commitIndex " + n.core.commitIndex()
                            + " but " + n.core.lastLogIndex() + " log entries");
                }
                if (n.applied.size() != n.core.commitIndex()) {
                    throw new Violation("liveness: " + n.id + " applied " + n.applied.size() + " of "
                            + n.core.commitIndex() + " committed entries");
                }
                for (String key : finalKeys) {
                    boolean found = false;
                    for (LogEntry e : n.applied) {
                        if (e.command().key().equals(key)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new Violation("liveness: " + n.id + " never applied " + key
                                + ", proposed after the heal");
                    }
                }
            }
        }
    }

    static List<Scenario> scenarios() {
        return List.of(
                new Scenario("1 node, crashes", 1, 10_000, 0.0, 0.0, 700, 0, 40, 30),
                new Scenario("2 nodes, mild faults", 2, 10_000, 0.05, 0.02, 1500, 1500, 40, 60),
                new Scenario("3 nodes, reliable network, frequent crashes", 3, 15_000, 0.0, 0.0, 600, 0, 30, 30),
                new Scenario("3 nodes, mild faults", 3, 15_000, 0.05, 0.02, 2500, 2500, 40, 60),
                new Scenario("3 nodes, heavy faults", 3, 15_000, 0.25, 0.10, 800, 700, 40, 250),
                new Scenario("5 nodes, partitions only", 5, 15_000, 0.02, 0.01, 0, 1200, 40, 80),
                new Scenario("5 nodes, mild faults", 5, 15_000, 0.05, 0.02, 2500, 2500, 40, 60),
                new Scenario("5 nodes, heavy faults", 5, 15_000, 0.25, 0.10, 700, 700, 40, 250));
    }

    public static void main(String[] args) {
        int runs = args.length > 0 ? Integer.parseInt(args[0]) : 100;
        long firstSeed = args.length > 1 ? Long.parseLong(args[1]) : 1;
        long started = System.nanoTime();
        long totalRuns = 0;
        for (Scenario sc : scenarios()) {
            long events = 0;
            long elections = 0;
            long committed = 0;
            long crashes = 0;
            long partitions = 0;
            for (long seed = firstSeed; seed < firstSeed + runs; seed++) {
                Run run = new Run(sc, seed);
                try {
                    run.execute();
                } catch (RuntimeException e) {
                    System.out.println("FAILED: " + sc.name() + "  seed=" + seed);
                    System.out.println("  " + e.getClass().getSimpleName() + ": " + e.getMessage());
                    System.out.println("  last events before the failure:");
                    for (String line : run.trace) {
                        System.out.println("    " + line);
                    }
                    System.out.println("  reproduce: java -cp out:out-test raftkv.ClusterSimulation 1 " + seed
                            + "   (scenario \"" + sc.name() + "\")");
                    System.exit(1);
                }
                events += run.events;
                elections += run.elections;
                committed += run.maxCommitted;
                crashes += run.crashes;
                partitions += run.partitions;
                totalRuns++;
            }
            System.out.printf("  ok  %-45s %d runs, %,d events, %,d elections, %,d entries committed, "
                            + "%,d crashes, %,d partitions%n",
                    sc.name(), runs, events, elections, committed, crashes, partitions);
        }
        System.out.printf("All %d simulated runs held every invariant (%.1fs).%n", totalRuns,
                (System.nanoTime() - started) / 1e9);
    }
}

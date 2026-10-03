package raftkv;

import raftkv.ClusterSimulation.Run;
import raftkv.ClusterSimulation.Scenario;
import raftkv.ClusterSimulation.SimNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Scripted scenarios on top of the cluster simulation that demonstrate what pre-vote, check-quorum and leader
 * stickiness buy, and what they cost. Every run also keeps checking the Raft safety invariants.
 *
 * Usage: java -cp out:out-test raftkv.ProtocolScenarios
 */
public final class ProtocolScenarios {

    private static final int SEEDS = 20;

    private static void require(boolean ok, String what) {
        if (!ok) {
            System.out.println("FAILED: " + what);
            System.exit(1);
        }
    }

    /** A quiet 3-node cluster: no loss, no random crashes or partitions, no client traffic. */
    private static Scenario quiet(boolean sticky) {
        return new Scenario(sticky ? "quiet, sticky" : "quiet", 3, 1_000_000_000, 0.0, 0.0, 0, 0, 1_000_000, 30, sticky);
    }

    private static SimNode leaderOf(Run run) {
        SimNode best = null;
        for (SimNode n : run.nodes) {
            if (n.up && n.core.state() == RaftCore.State.LEADER
                    && (best == null || n.core.currentTerm() > best.core.currentTerm())) {
                best = n;
            }
        }
        return best;
    }

    private static SimNode otherLeader(Run run, SimNode except) {
        for (SimNode n : run.nodes) {
            if (n != except && n.up && n.core.state() == RaftCore.State.LEADER) {
                return n;
            }
        }
        return null;
    }

    private static SimNode someFollower(Run run, SimNode leader) {
        for (SimNode n : run.nodes) {
            if (n != leader) {
                return n;
            }
        }
        throw new IllegalStateException("no follower");
    }

    /** A follower cut off from the majority for 10 s must not inflate its term, and what happens on rejoin. */
    private static int isolatedFollower(boolean sticky) {
        int disruptedRejoins = 0;
        for (long seed = 1; seed <= SEEDS; seed++) {
            Run run = new Run(quiet(sticky), seed);
            run.start();
            run.runUntil(3_000);
            SimNode leader = leaderOf(run);
            require(leader != null, "no leader after 3s (seed " + seed + ")");
            long term = leader.core.currentTerm();
            SimNode follower = someFollower(run, leader);

            run.group.put(follower.id, 1); // cut the follower off
            run.runUntil(run.now + 10_000);
            require(follower.core.currentTerm() == term, "isolated " + follower.id + " inflated its term from " + term
                    + " to " + follower.core.currentTerm() + " (seed " + seed + ")");
            require(leader.core.state() == RaftCore.State.LEADER && leader.core.currentTerm() == term,
                    "the majority side lost its leader while one follower was away (seed " + seed + ")");

            run.setAllGroups(0); // rejoin
            run.runUntil(run.now + 3_000);
            boolean undisturbed = leader.core.state() == RaftCore.State.LEADER && leader.core.currentTerm() == term
                    && follower.core.state() == RaftCore.State.FOLLOWER && leader.id.equals(follower.core.leaderId());
            if (sticky) {
                require(undisturbed, "sticky leader was disturbed by a rejoining node (seed " + seed + ")");
            } else if (!undisturbed) {
                disruptedRejoins++;
            }
            run.finalChecks();
        }
        return disruptedRejoins;
    }

    /** A leader cut off from the majority must stop leading, and the majority must elect a replacement. */
    private static long isolatedLeader(boolean sticky) {
        long slowest = 0;
        for (long seed = 1; seed <= SEEDS; seed++) {
            Run run = new Run(quiet(sticky), seed);
            run.start();
            run.runUntil(3_000);
            SimNode old = leaderOf(run);
            require(old != null, "no leader after 3s (seed " + seed + ")");
            long oldTerm = old.core.currentTerm();
            long cutAt = run.now;

            run.group.put(old.id, 1);
            while (old.core.state() == RaftCore.State.LEADER && run.now < cutAt + 2_000) {
                run.runUntil(run.now + 5);
            }
            long tookMs = run.now - cutAt;
            require(old.core.state() != RaftCore.State.LEADER, "the cut-off leader never stepped down (seed " + seed + ")");
            require(tookMs <= 800, "the cut-off leader needed " + tookMs + " ms to step down (seed " + seed + ")");
            slowest = Math.max(slowest, tookMs);

            run.runUntil(run.now + 2_000);
            SimNode replacement = otherLeader(run, old);
            require(replacement != null && replacement.core.currentTerm() > oldTerm,
                    "the majority did not elect a replacement leader (seed " + seed + ")");
            require(old.core.currentTerm() <= replacement.core.currentTerm(),
                    "the cut-off node's term ran ahead of the cluster (seed " + seed + ")");
            run.setAllGroups(0);
            run.runUntil(run.now + 3_000);
            run.finalChecks();
        }
        return slowest;
    }

    /** Simulated time from the leader's crash until another node is leader, over many runs. */
    private static List<Long> failoverTimes(boolean sticky, int runs) {
        List<Long> times = new ArrayList<>();
        for (long seed = 1; seed <= runs; seed++) {
            Run run = new Run(quiet(sticky), seed);
            run.start();
            run.runUntil(3_000);
            SimNode old = leaderOf(run);
            require(old != null, "no leader after 3s (seed " + seed + ")");
            run.crash(old);
            long crashedAt = run.now;
            long took = -1;
            for (long t = 1; t <= 3_000; t++) {
                run.runUntil(crashedAt + t);
                if (otherLeader(run, old) != null) {
                    took = t;
                    break;
                }
            }
            require(took >= 0, "no new leader within 3 s of the crash (seed " + seed + ")");
            times.add(took);
        }
        Collections.sort(times);
        return times;
    }

    private static String summary(List<Long> sorted) {
        double mean = sorted.stream().mapToLong(Long::longValue).average().orElse(0);
        return String.format("median %d ms, mean %.0f ms, p95 %d ms, max %d ms", sorted.get(sorted.size() / 2), mean,
                sorted.get((int) (sorted.size() * 0.95)), sorted.get(sorted.size() - 1));
    }

    public static void main(String[] args) {
        System.out.println("Pre-vote: a follower isolated for 10 s keeps its term");
        int disrupted = isolatedFollower(false);
        System.out.printf("  ok  term unchanged in all %d runs; on rejoin the leader was deposed in %d of %d runs "
                + "(leader stickiness off)%n", SEEDS, disrupted, SEEDS);
        isolatedFollower(true);
        System.out.printf("  ok  with leader stickiness the leader was never disturbed by a rejoining node (%d runs)%n", SEEDS);

        System.out.println("Check-quorum: a leader cut off from the majority steps down");
        long slowest = isolatedLeader(false);
        System.out.printf("  ok  stepped down in every run, slowest %d ms; the majority elected a replacement%n", slowest);
        isolatedLeader(true);
        System.out.println("  ok  same with leader stickiness on");

        System.out.println("Failover after a leader crash (simulated network latency 1-20 ms, 200 runs each)");
        System.out.println("  stickiness off: " + summary(failoverTimes(false, 200)));
        System.out.println("  stickiness on : " + summary(failoverTimes(true, 200)));
        System.out.println("All protocol scenarios passed.");
    }
}

package raftkv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import raftkv.Messages.AppendEntriesRequest;
import raftkv.Messages.AppendEntriesResponse;
import raftkv.Messages.RequestVoteRequest;
import raftkv.Messages.RequestVoteResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runtime shell around {@link RaftCore}: owns the timers, the disk (WAL + hard state), the HTTP server
 * and the outbound HTTP client. All access to the core happens under {@link #lock}, and the core's
 * actions (including fsyncs) are executed under that same lock, so state is durable before any reply
 * is sent and before any message leaves this node.
 */
public final class RaftNodeServer {

    private static final int HEARTBEAT_MS = 50;
    private static final int ELECTION_MIN_MS = 150;
    private static final int ELECTION_MAX_MS = 350;
    private static final int COMMIT_WAIT_MS = 2000;
    private static final int RPC_TIMEOUT_MS = 400;
    private static final int MAX_VALUE_BYTES = 1 << 20;
    private static final String KEYS_PREFIX = "/v1/keys/";

    private record PendingWrite(long term, CompletableFuture<Long> future) {
    }

    private enum WriteStatus { COMMITTED, NOT_LEADER, TIMEOUT, LEADERSHIP_LOST }

    @FunctionalInterface
    private interface ExchangeHandler {
        void handle(HttpExchange exchange) throws Exception;
    }

    private final String id;
    private final int port;
    private final String selfAddress;
    private final boolean leaderStickiness;
    private final Map<String, String> peerAddresses;

    private final ReentrantLock lock = new ReentrantLock();
    private final RaftCore core;
    private final BinaryWAL wal;
    private final StateStore stateStore;
    private final KVStore kv = new KVStore();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ExecutorService rpcExecutor = Executors.newCachedThreadPool();
    private final HttpClient httpClient;
    private HttpServer httpServer;

    private final ConcurrentHashMap<Long, PendingWrite> pending = new ConcurrentHashMap<>();
    private ScheduledFuture<?> electionFuture;
    private long electionEpoch;
    private volatile boolean stopped;

    private RaftCore.State lastLoggedState;
    private long lastLoggedTerm = -1;

    public RaftNodeServer(String id, int port, Map<String, String> peerAddresses, Path dataDir,
                          boolean leaderStickiness) throws IOException {
        this.id = id;
        this.leaderStickiness = leaderStickiness;
        this.port = port;
        this.selfAddress = "localhost:" + port;
        this.peerAddresses = Map.copyOf(peerAddresses);
        this.stateStore = new StateStore(dataDir);
        this.wal = new BinaryWAL(dataDir);
        StateStore.HardState hardState = stateStore.load();
        List<LogEntry> recovered = wal.recoverAll();
        this.core = new RaftCore(id, this.peerAddresses.keySet(), hardState.currentTerm(),
                hardState.votedFor(), recovered, leaderStickiness);
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(200))
                .executor(rpcExecutor)
                .build();
        log("recovered term=%d votedFor=%s logEntries=%d stickyLeader=%s", hardState.currentTerm(),
                hardState.votedFor(), recovered.size(), leaderStickiness);
    }

    // ------------------------------------------------------------------ lifecycle

    public void start() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.setExecutor(Executors.newCachedThreadPool());
        httpServer.createContext("/raft/pre-vote", safe(this::handlePreVoteHttp));
        httpServer.createContext("/raft/request-vote", safe(this::handleRequestVoteHttp));
        httpServer.createContext("/raft/append-entries", safe(this::handleAppendEntriesHttp));
        httpServer.createContext(KEYS_PREFIX, safe(this::handleKeysHttp));
        httpServer.createContext("/v1/status", safe(this::handleStatusHttp));
        httpServer.start();

        lock.lock();
        try {
            resetElectionTimer();
        } finally {
            lock.unlock();
        }
        scheduler.scheduleAtFixedRate(this::heartbeatTick, HEARTBEAT_MS, HEARTBEAT_MS, TimeUnit.MILLISECONDS);
        log("listening on %s, peers=%s", selfAddress, peerAddresses);
    }

    public void stop() {
        stopped = true;
        if (httpServer != null) {
            httpServer.stop(0);
        }
        scheduler.shutdownNow();
        rpcExecutor.shutdownNow();
        try {
            wal.close();
        } catch (IOException e) {
            log("error closing WAL: %s", e);
        }
    }

    // ------------------------------------------------------------------ timers

    /** Caller must hold {@link #lock}. */
    private void resetElectionTimer() {
        if (stopped) {
            return;
        }
        if (electionFuture != null) {
            electionFuture.cancel(false);
        }
        final long epoch = ++electionEpoch;
        int delay = ThreadLocalRandom.current().nextInt(ELECTION_MIN_MS, ELECTION_MAX_MS + 1);
        try {
            electionFuture = scheduler.schedule(() -> electionTimerFired(epoch), delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            electionFuture = null;
        }
    }

    private void electionTimerFired(long epoch) {
        try {
            lock.lock();
            try {
                if (stopped || epoch != electionEpoch) {
                    return; // superseded by a reset that raced with this firing
                }
                executeActions(core.onElectionTimeout());
            } finally {
                lock.unlock();
            }
        } catch (Throwable t) {
            log("election timer error: %s", t);
        }
    }

    private void heartbeatTick() {
        try {
            lock.lock();
            try {
                if (!stopped) {
                    executeActions(core.onHeartbeatTick());
                }
            } finally {
                lock.unlock();
            }
        } catch (Throwable t) {
            log("heartbeat error: %s", t);
        }
    }

    // ------------------------------------------------------------------ action execution

    /** Executes core actions in order. Caller must hold {@link #lock}. */
    private void executeActions(List<Action> actions) {
        for (Action action : actions) {
            try {
                if (action instanceof Action.PersistHardState a) {
                    stateStore.save(a.term(), a.votedFor());
                } else if (action instanceof Action.AppendToLog a) {
                    wal.appendAll(a.entries());
                } else if (action instanceof Action.TruncateLogFrom a) {
                    wal.truncateSuffix(a.index());
                } else if (action instanceof Action.ApplyEntries a) {
                    applyEntries(a.entries());
                } else if (action instanceof Action.SendPreVote a) {
                    sendPreVote(a.peerId(), a.request());
                } else if (action instanceof Action.SendRequestVote a) {
                    sendRequestVote(a.peerId(), a.request());
                } else if (action instanceof Action.SendAppendEntries a) {
                    sendAppendEntries(a.peerId(), a.request());
                } else if (action instanceof Action.ResetElectionTimer) {
                    resetElectionTimer();
                } else {
                    throw new IllegalStateException("unhandled action " + action);
                }
            } catch (IOException e) {
                fatal("durable storage failure while executing " + action.getClass().getSimpleName(), e);
            }
        }
        noteStateChange();
    }

    private void applyEntries(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            kv.apply(entry.command());
            PendingWrite waiter = pending.remove(entry.index());
            if (waiter != null) {
                waiter.future().complete(entry.term());
            }
        }
    }

    private void noteStateChange() {
        if (core.state() != lastLoggedState || core.currentTerm() != lastLoggedTerm) {
            lastLoggedState = core.state();
            lastLoggedTerm = core.currentTerm();
            log("state=%s term=%d leader=%s", lastLoggedState, lastLoggedTerm, core.leaderId());
        }
    }

    // ------------------------------------------------------------------ outbound RPCs

    private void sendPreVote(String peerId, RequestVoteRequest poll) {
        post(peerId, "/raft/pre-vote", poll.encode(), body -> {
            RequestVoteResponse response = RequestVoteResponse.decode(body);
            lock.lock();
            try {
                executeActions(core.handlePreVoteResponse(peerId, poll, response));
            } finally {
                lock.unlock();
            }
        });
    }

    private void sendRequestVote(String peerId, RequestVoteRequest request) {
        post(peerId, "/raft/request-vote", request.encode(), body -> {
            RequestVoteResponse response = RequestVoteResponse.decode(body);
            lock.lock();
            try {
                executeActions(core.handleRequestVoteResponse(peerId, response));
            } finally {
                lock.unlock();
            }
        });
    }

    private void sendAppendEntries(String peerId, AppendEntriesRequest request) {
        post(peerId, "/raft/append-entries", request.encode(), body -> {
            AppendEntriesResponse response = AppendEntriesResponse.decode(body);
            lock.lock();
            try {
                executeActions(core.handleAppendEntriesResponse(peerId, request, response));
            } finally {
                lock.unlock();
            }
        });
    }

    @FunctionalInterface
    private interface ResponseConsumer {
        void accept(byte[] body) throws IOException;
    }

    private void post(String peerId, String path, byte[] payload, ResponseConsumer consumer) {
        String address = peerAddresses.get(peerId);
        if (address == null || stopped) {
            return;
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + address + path))
                .timeout(Duration.ofMillis(RPC_TIMEOUT_MS))
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                .build();
        try {
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                    .whenComplete((response, error) -> {
                        if (error != null || response == null || response.statusCode() != 200) {
                            return; // unreachable peer: Raft tolerates lost messages
                        }
                        try {
                            consumer.accept(response.body());
                        } catch (IOException e) {
                            log("malformed reply from %s: %s", peerId, e);
                        } catch (Throwable t) {
                            log("error handling reply from %s: %s", peerId, t);
                        }
                    });
        } catch (RejectedExecutionException ignored) {
            // shutting down
        }
    }

    // ------------------------------------------------------------------ inbound Raft RPCs

    private void handlePreVoteHttp(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            sendText(ex, 405, "POST only\n");
            return;
        }
        RequestVoteRequest request;
        try {
            request = RequestVoteRequest.decode(ex.getRequestBody().readAllBytes());
        } catch (IOException e) {
            sendText(ex, 400, "malformed request\n");
            return;
        }
        RaftCore.Outcome<RequestVoteResponse> outcome;
        lock.lock();
        try {
            outcome = core.handlePreVote(request); // a poll: changes no state, so nothing to persist
        } finally {
            lock.unlock();
        }
        sendBytes(ex, 200, outcome.value().encode());
    }

    private void handleRequestVoteHttp(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            sendText(ex, 405, "POST only\n");
            return;
        }
        RequestVoteRequest request;
        try {
            request = RequestVoteRequest.decode(ex.getRequestBody().readAllBytes());
        } catch (IOException e) {
            sendText(ex, 400, "malformed request\n");
            return;
        }
        RaftCore.Outcome<RequestVoteResponse> outcome;
        lock.lock();
        try {
            outcome = core.handleRequestVote(request);
            executeActions(outcome.actions()); // vote is durable before the reply is sent
        } finally {
            lock.unlock();
        }
        sendBytes(ex, 200, outcome.value().encode());
    }

    private void handleAppendEntriesHttp(HttpExchange ex) throws IOException {
        if (!"POST".equals(ex.getRequestMethod())) {
            sendText(ex, 405, "POST only\n");
            return;
        }
        AppendEntriesRequest request;
        try {
            request = AppendEntriesRequest.decode(ex.getRequestBody().readAllBytes());
        } catch (IOException e) {
            sendText(ex, 400, "malformed request\n");
            return;
        }
        RaftCore.Outcome<AppendEntriesResponse> outcome;
        lock.lock();
        try {
            outcome = core.handleAppendEntries(request);
            executeActions(outcome.actions()); // entries are fsynced before the ack is sent
        } finally {
            lock.unlock();
        }
        sendBytes(ex, 200, outcome.value().encode());
    }

    // ------------------------------------------------------------------ client API

    private void handleKeysHttp(HttpExchange ex) throws Exception {
        String path = ex.getRequestURI().getPath();
        if (!path.startsWith(KEYS_PREFIX) || path.length() == KEYS_PREFIX.length()) {
            sendText(ex, 400, "usage: /v1/keys/{key}\n");
            return;
        }
        String key = path.substring(KEYS_PREFIX.length());
        switch (ex.getRequestMethod()) {
            case "GET" -> handleGet(ex, key);
            case "PUT" -> {
                byte[] body = ex.getRequestBody().readAllBytes();
                if (body.length > MAX_VALUE_BYTES) {
                    sendText(ex, 413, "value too large\n");
                    return;
                }
                handleWrite(ex, Command.put(key, new String(body, StandardCharsets.UTF_8)));
            }
            case "DELETE" -> handleWrite(ex, Command.delete(key));
            default -> sendText(ex, 405, "GET, PUT and DELETE only\n");
        }
    }

    /**
     * Default: serve the locally applied state (fast; a follower may lag the leader by up to one heartbeat).
     * With ?consistent=true the request goes to the leader, which commits a no-op through the log first
     * (a log-based read barrier), so the read observes every write committed before it.
     */
    private void handleGet(HttpExchange ex, String key) throws Exception {
        String query = ex.getRequestURI().getRawQuery();
        if (query != null && query.contains("consistent=true")) {
            WriteStatus status = proposeAndWait(Command.noop());
            if (status != WriteStatus.COMMITTED) {
                respondToFailedProposal(ex, status);
                return;
            }
        }
        Optional<String> value = kv.get(key);
        if (value.isPresent()) {
            sendText(ex, 200, value.get());
        } else {
            sendText(ex, 404, "key not found\n");
        }
    }

    private void handleWrite(HttpExchange ex, Command command) throws Exception {
        WriteStatus status = proposeAndWait(command);
        if (status == WriteStatus.COMMITTED) {
            sendText(ex, 200, "OK\n");
        } else {
            respondToFailedProposal(ex, status);
        }
    }

    private void respondToFailedProposal(HttpExchange ex, WriteStatus status) throws IOException {
        switch (status) {
            case NOT_LEADER -> {
                String leader = leaderAddress();
                if (leader == null) {
                    ex.getResponseHeaders().set("Retry-After", "1");
                    sendText(ex, 503, "no leader elected yet; retry shortly\n");
                } else {
                    String query = ex.getRequestURI().getRawQuery();
                    String location = "http://" + leader + ex.getRequestURI().getRawPath()
                            + (query == null ? "" : "?" + query);
                    ex.getResponseHeaders().set("Location", location);
                    sendText(ex, 307, "not the leader; redirecting to " + location + "\n");
                }
            }
            case TIMEOUT -> sendText(ex, 503, "timed out waiting for quorum commit\n");
            case LEADERSHIP_LOST -> sendText(ex, 503, "leadership changed before commit; retry\n");
            case COMMITTED -> sendText(ex, 200, "OK\n");
        }
    }

    /** Proposes a command on the leader and waits up to 2s for it to commit and be applied. */
    private WriteStatus proposeAndWait(Command command) throws InterruptedException {
        PendingWrite waiter = null;
        long index = -1;
        lock.lock();
        try {
            RaftCore.Outcome<RaftCore.ProposeResult> outcome = core.propose(command);
            if (!outcome.value().accepted()) {
                return WriteStatus.NOT_LEADER;
            }
            index = outcome.value().index();
            waiter = new PendingWrite(outcome.value().term(), new CompletableFuture<>());
            pending.put(index, waiter); // registered before execution: a single-node commit completes inline
            executeActions(outcome.actions());
        } finally {
            lock.unlock();
        }
        try {
            long appliedTerm = waiter.future().get(COMMIT_WAIT_MS, TimeUnit.MILLISECONDS);
            // If a different leader overwrote this index, the entry that got applied has another term.
            return appliedTerm == waiter.term() ? WriteStatus.COMMITTED : WriteStatus.LEADERSHIP_LOST;
        } catch (TimeoutException e) {
            pending.remove(index, waiter);
            return WriteStatus.TIMEOUT;
        } catch (ExecutionException e) {
            pending.remove(index, waiter);
            return WriteStatus.TIMEOUT;
        }
    }

    private String leaderAddress() {
        lock.lock();
        try {
            String leader = core.leaderId();
            if (leader == null) {
                return null;
            }
            return leader.equals(id) ? selfAddress : peerAddresses.get(leader);
        } finally {
            lock.unlock();
        }
    }

    private void handleStatusHttp(HttpExchange ex) throws IOException {
        String text;
        lock.lock();
        try {
            text = String.format("id=%s state=%s term=%d leader=%s commitIndex=%d lastLogIndex=%d keys=%d%n",
                    id, core.state(), core.currentTerm(),
                    core.leaderId() == null ? "none" : core.leaderId(),
                    core.commitIndex(), core.lastLogIndex(), kv.size());
        } finally {
            lock.unlock();
        }
        sendText(ex, 200, text);
    }

    // ------------------------------------------------------------------ HTTP helpers

    private HttpHandler safe(ExchangeHandler handler) {
        return exchange -> {
            try {
                handler.handle(exchange);
            } catch (Throwable t) {
                log("request %s %s failed: %s", exchange.getRequestMethod(), exchange.getRequestURI(), t);
                try {
                    sendText(exchange, 500, "internal error\n");
                } catch (Exception ignored) {
                    // response already started
                }
            } finally {
                exchange.close();
            }
        };
    }

    private static void sendText(HttpExchange ex, int status, String body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        sendBody(ex, status, body.getBytes(StandardCharsets.UTF_8));
    }

    private static void sendBytes(HttpExchange ex, int status, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
        sendBody(ex, status, body);
    }

    private static void sendBody(HttpExchange ex, int status, byte[] body) throws IOException {
        if (body.length == 0) {
            ex.sendResponseHeaders(status, -1);
            return;
        }
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(body);
        }
    }

    // ------------------------------------------------------------------ misc

    private void fatal(String message, Throwable cause) {
        System.err.println(Instant.now() + " [" + id + "] FATAL: " + message + ": " + cause);
        cause.printStackTrace();
        Runtime.getRuntime().halt(1); // never continue after losing the ability to persist
    }

    private void log(String format, Object... args) {
        System.out.println(Instant.now() + " [" + id + "] " + String.format(format, args));
    }

    // ------------------------------------------------------------------ entry point

    public static void main(String[] args) throws Exception {
        Map<String, String> options = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-") && i + 1 < args.length) {
                options.put(args[i], args[++i]);
            } else {
                usage("unexpected argument: " + args[i]);
            }
        }
        String id = options.get("-id");
        String portText = options.get("-port");
        String dataText = options.get("-data");
        if (id == null || portText == null || dataText == null) {
            usage("-id, -port and -data are required");
        }
        int port = 0;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException e) {
            usage("-port must be an integer");
        }

        Map<String, String> peers = new LinkedHashMap<>();
        String peerText = options.getOrDefault("-peers", "");
        for (String token : peerText.split(",")) {
            token = token.trim();
            if (token.isEmpty()) {
                continue;
            }
            int eq = token.indexOf('=');
            if (eq <= 0 || eq == token.length() - 1) {
                usage("bad peer '" + token + "', expected id=host:port");
            }
            String peerId = token.substring(0, eq);
            if (peerId.equals(id)) {
                usage("-peers must not include this node (" + id + ")");
            }
            peers.put(peerId, token.substring(eq + 1));
        }

        boolean sticky = Boolean.parseBoolean(options.getOrDefault("-sticky", "false"));
        RaftNodeServer server = new RaftNodeServer(id, port, peers, Paths.get(dataText), sticky);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
    }

    private static void usage(String problem) {
        System.err.println(problem);
        System.err.println("usage: RaftNodeServer -id node1 -port 8051 "
                + "-peers node2=localhost:8052,node3=localhost:8053 -data ./data/node1 [-sticky true]");
        System.exit(2);
    }
}

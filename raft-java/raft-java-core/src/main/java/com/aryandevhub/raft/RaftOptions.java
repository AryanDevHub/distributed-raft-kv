package com.aryandevhub.raft;

public class RaftOptions {

    private int electionTimeoutMilliseconds = 5000;
    private int heartbeatPeriodMilliseconds = 500;
    private int snapshotPeriodSeconds = 3600;
    private int snapshotMinLogSize = 100 * 1024 * 1024;
    private int maxSnapshotBytesPerRequest = 500 * 1024;
    private int maxLogEntriesPerRequest = 5000;
    private int maxSegmentFileSize = 100 * 1000 * 1000;
    private long catchupMargin = 500;
    private long maxAwaitTimeout = 1000;
    private int raftConsensusThreadNum = 20;
    private boolean asyncWrite = false;
    private String dataDir = System.getProperty("com.aryandevhub.raft.data.dir");

    // Getter and Setter for electionTimeoutMilliseconds
    public int getElectionTimeoutMilliseconds() {
        return electionTimeoutMilliseconds;
    }
    public void setElectionTimeoutMilliseconds(int electionTimeoutMilliseconds) {
        this.electionTimeoutMilliseconds = electionTimeoutMilliseconds;
    }

    // Getter and Setter for heartbeatPeriodMilliseconds
    public int getHeartbeatPeriodMilliseconds() {
        return heartbeatPeriodMilliseconds;
    }
    public void setHeartbeatPeriodMilliseconds(int heartbeatPeriodMilliseconds) {
        this.heartbeatPeriodMilliseconds = heartbeatPeriodMilliseconds;
    }

    // Getter and Setter for snapshotPeriodSeconds
    public int getSnapshotPeriodSeconds() {
        return snapshotPeriodSeconds;
    }
    public void setSnapshotPeriodSeconds(int snapshotPeriodSeconds) {
        this.snapshotPeriodSeconds = snapshotPeriodSeconds;
    }

    // Getter and Setter for snapshotMinLogSize
    public int getSnapshotMinLogSize() {
        return snapshotMinLogSize;
    }
    public void setSnapshotMinLogSize(int snapshotMinLogSize) {
        this.snapshotMinLogSize = snapshotMinLogSize;
    }

    // Getter and Setter for maxSnapshotBytesPerRequest
    public int getMaxSnapshotBytesPerRequest() {
        return maxSnapshotBytesPerRequest;
    }
    public void setMaxSnapshotBytesPerRequest(int maxSnapshotBytesPerRequest) {
        this.maxSnapshotBytesPerRequest = maxSnapshotBytesPerRequest;
    }

    // Getter and Setter for maxLogEntriesPerRequest
    public int getMaxLogEntriesPerRequest() {
        return maxLogEntriesPerRequest;
    }
    public void setMaxLogEntriesPerRequest(int maxLogEntriesPerRequest) {
        this.maxLogEntriesPerRequest = maxLogEntriesPerRequest;
    }

    // Getter and Setter for maxSegmentFileSize
    public int getMaxSegmentFileSize() {
        return maxSegmentFileSize;
    }
    public void setMaxSegmentFileSize(int maxSegmentFileSize) {
        this.maxSegmentFileSize = maxSegmentFileSize;
    }

    // Getter and Setter for catchupMargin
    public long getCatchupMargin() {
        return catchupMargin;
    }
    public void setCatchupMargin(long catchupMargin) {
        this.catchupMargin = catchupMargin;
    }

    // Getter and Setter for maxAwaitTimeout
    public long getMaxAwaitTimeout() {
        return maxAwaitTimeout;
    }
    public void setMaxAwaitTimeout(long maxAwaitTimeout) {
        this.maxAwaitTimeout = maxAwaitTimeout;
    }

    // Getter and Setter for raftConsensusThreadNum
    public int getRaftConsensusThreadNum() {
        return raftConsensusThreadNum;
    }
    public void setRaftConsensusThreadNum(int raftConsensusThreadNum) {
        this.raftConsensusThreadNum = raftConsensusThreadNum;
    }

    // Getter and Setter for asyncWrite
    public boolean isAsyncWrite() {
        return asyncWrite;
    }
    public void setAsyncWrite(boolean asyncWrite) {
        this.asyncWrite = asyncWrite;
    }

    // Getter and Setter for dataDir
    public String getDataDir() {
        return dataDir;
    }
    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }
}
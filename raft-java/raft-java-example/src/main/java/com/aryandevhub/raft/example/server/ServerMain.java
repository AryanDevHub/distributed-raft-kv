package com.aryandevhub.raft.example.server;

import com.baidu.brpc.server.RpcServer;
import com.aryandevhub.raft.RaftOptions;
import com.aryandevhub.raft.example.server.service.ExampleService;
import com.aryandevhub.raft.RaftNode;
import com.aryandevhub.raft.example.server.service.impl.ExampleServiceImpl;
import com.aryandevhub.raft.proto.RaftProto;
import com.aryandevhub.raft.service.RaftClientService;
import com.aryandevhub.raft.service.RaftConsensusService;
import com.aryandevhub.raft.service.impl.RaftClientServiceImpl;
import com.aryandevhub.raft.service.impl.RaftConsensusServiceImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
public class ServerMain {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.printf("Usage: ./run_server.sh DATA_PATH CLUSTER CURRENT_NODE\n");
            System.exit(-1);
        }
        // parse args
        // raft data dir
        String dataPath = args[0];
        // peers, format is "host:port:serverId,host2:port2:serverId2"
        String servers = args[1];
        String[] splitArray = servers.split(",");
        List<RaftProto.Server> serverList = new ArrayList<>();
        for (String serverString : splitArray) {
            RaftProto.Server server = parseServer(serverString);
            serverList.add(server);
        }
        // local server
        RaftProto.Server localServer = parseServer(args[2]);

        
        RpcServer server = new RpcServer(localServer.getEndpoint().getPort());
        
        // just for test snapshot
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setDataDir(dataPath);
        raftOptions.setSnapshotMinLogSize(10 * 1024);
        raftOptions.setSnapshotPeriodSeconds(30);
        raftOptions.setMaxSegmentFileSize(1024 * 1024);
        
        ExampleStateMachine stateMachine = new ExampleStateMachine(raftOptions.getDataDir());
        
        RaftNode raftNode = new RaftNode(raftOptions, serverList, localServer, stateMachine);
        
        RaftConsensusService raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        server.registerService(raftConsensusService);
        
        RaftClientService raftClientService = new RaftClientServiceImpl(raftNode);
        server.registerService(raftClientService);
        
        ExampleService exampleService = new ExampleServiceImpl(raftNode, stateMachine);
        server.registerService(exampleService);
        
        server.start();
        raftNode.init();
    }

    private static RaftProto.Server parseServer(String serverString) {
        String[] splitServer = serverString.split(":");
        String host = splitServer[0];
        Integer port = Integer.parseInt(splitServer[1]);
        Integer serverId = Integer.parseInt(splitServer[2]);
        RaftProto.Endpoint endPoint = RaftProto.Endpoint.newBuilder()
                .setHost(host).setPort(port).build();
        RaftProto.Server.Builder serverBuilder = RaftProto.Server.newBuilder();
        RaftProto.Server server = serverBuilder.setServerId(serverId).setEndpoint(endPoint).build();
        return server;
    }
}

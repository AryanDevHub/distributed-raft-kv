package com.aryandevhub.raft.service;

import com.aryandevhub.raft.proto.RaftProto;


public interface RaftClientService {

    
    RaftProto.GetLeaderResponse getLeader(RaftProto.GetLeaderRequest request);

    
    RaftProto.GetConfigurationResponse getConfiguration(RaftProto.GetConfigurationRequest request);

    
    RaftProto.AddPeersResponse addPeers(RaftProto.AddPeersRequest request);

    
    RaftProto.RemovePeersResponse removePeers(RaftProto.RemovePeersRequest request);
}

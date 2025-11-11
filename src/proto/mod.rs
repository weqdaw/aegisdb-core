pub mod kvrpcpb;
pub mod metapb {
    tonic::include_proto!("metapb");
}
pub mod errorpb {
    tonic::include_proto!("errorpb");
}
pub mod schedulerpb {
    tonic::include_proto!("schedulerpb");
}
pub mod eraftpb;
pub mod raft_cmdpb;
package net.thumbtack;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class WriteActionStrategy implements ActionStrategy {
    private BucketService bucketService;
    private ShardService shardService;
    private AdminService adminService;

    @Override
    public Result doAction(int bucketIndex, Action action) throws BucketServiceException {
        // TODO extract logic to ActionStrategy hierarchy.
        Result result = null;
        if (action.getActionType() != ActionType.READ) {
            ActionStorage.getInstance().addAction(bucketIndex, action);
        }
        // here can return OK, and do other in background.
        try {
            bucketService.lockBucketIndex(bucketIndex);
//            Shard activeShard = findShard();
            String activeShardId = shardService.findActiveShardId(bucketIndex);
            try {
                bucketService.doAction(activeShardId, action);
            } catch (BucketServiceException e) {
                adminService.removeShard(activeShardId); // TODO run in separate thread, because bucket switching may be required, and bucket may be locked to changing.
                throw new BucketServiceException(e);
            }

            // here only for WRITE actions.
            for (String replicaShardId : bucketService.findReplicaShardIds(bucketIndex)) {
                // may be write to any replicas it is minor error and do some internal stuff without client information. extract logic to ReplicaActionStrategy
                try {
                    bucketService.doAction(replicaShardId, action);
                }  catch (BucketServiceException e) {
                    adminService.removeShard(activeShardId); // TODO run in separate thread, because bucket switching may be required, and bucket may be locked to changing.
                    throw new BucketServiceException(e);
                }
            }
            // TODO should we merge any activeShard ReplicaShards results ? seems like shouldn't.

        } finally {
            bucketService.unlockBucketIndex(bucketIndex);
        }
        return result;
    }

    @Override
    public Result doAction(Action action) throws BucketServiceException {
        throw new UnsupportedOperationException();
    }
}

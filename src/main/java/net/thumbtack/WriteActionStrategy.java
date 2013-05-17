package net.thumbtack;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class WriteActionStrategy implements ActionStrategy {
    private BucketService bucketService;
    private ShardService shardService;

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
            bucketService.doAction(activeShardId, action);
            // here only for WRITE actions.
            for (String replicaShardId : bucketService.findReplicaShardIds(bucketIndex)) {
                // may be write to any replicas it is minor error and do some internal stuff without client information. extract logic to ReplicaActionStrategy
                bucketService.doAction(replicaShardId, action);
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

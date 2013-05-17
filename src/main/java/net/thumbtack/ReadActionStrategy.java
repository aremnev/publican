package net.thumbtack;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ReadActionStrategy implements ActionStrategy {
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
            }  catch (BucketServiceException e) {
                adminService.removeShard(activeShardId); // TODO run in separate thread, because bucket switching may be required, and bucket may be locked to changing.
                throw new BucketServiceException(e);
            }

        } finally {
            bucketService.unlockBucketIndex(bucketIndex);
        }
        return result;
    }

    @Override
    public Result doAction(Action action) throws BucketServiceException {
        // search we can perform without id (by attributes), and without write lock.
        if (action.getActionType() != ActionType.READ) {
            throw new BucketServiceException("method should be used for READ actions only.");
        }
        ResultBuilderFactory resultBuilderFactory = ResultBuilderFactoryImpl.getInstance();
        ResultBuilder resultBuilder = resultBuilderFactory.createResultBuilderForAction(action);
        Result result = null;
        Iterator<Integer> allBucketIndexIterator = bucketService.getAllBucketIndexIterator();
        while (allBucketIndexIterator.hasNext()) {
            Integer bucketIndex = allBucketIndexIterator.next();
            resultBuilder.addResult(filterDataFromNonActiveBuckets(bucketIndex, doAction(bucketIndex, action)));
        }
        return resultBuilder.build();
    }

    /**
     * when Action searches data in shard, it know nothing about buckets areas and collects data from all of them (A, P, D, R), so
     * here we have to filter retrieved data from non-active buckets, and return data from active buckets only.
     * not delete, useful for read over all shards.
     * @param bucketIndex
     * @param result
     * @return
     */
    private Result filterDataFromNonActiveBuckets(int bucketIndex, Result result) {
        Map<Long, Object> resultMap = new HashMap<Long, Object>();
        Map<Long, Object> entityIdEntityMap = result.getEntityIdEntityMap();
        for (Map.Entry<Long, Object> entry : entityIdEntityMap.entrySet()) {
            if (bucketService.mapEntityIdToBucketIndex(entry.getKey()) == bucketIndex) {
                resultMap.put(entry.getKey(), entry.getValue());
            }
        }
        result.setEntityIdEntityMap(resultMap);
        return result;
    }
}

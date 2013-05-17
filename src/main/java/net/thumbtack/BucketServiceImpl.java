package net.thumbtack;

import net.thumbtack.sharding.core.Shard;
import net.thumbtack.sharding.core.query.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * NB BucketServiceImpl can be running on many machines in same time, so it should be able to synchronize they work correctly.
 */
public class BucketServiceImpl implements BucketService {
    private static final Logger logger = LoggerFactory.getLogger(BucketServiceImpl.class);
    private static final int MAX_LOCK_COUNT = 10;
    private static final int THRESHOLD = 10;
    private static final int TIMEOUT = 1000;

    @Override
    public Result doAction(Action action, long entityId) throws BucketServiceException {
        return doAction(mapEntityIdToBucketIndex(entityId), action);
    }

    @Override
    public Result doReadAction(Action action) throws BucketServiceException {
        // search we can perform without id (by attributes), and without write lock.
        if (action.getActionType() != ActionType.READ) {
            throw new BucketServiceException("method should be used for READ actions only.");
        }
        ResultBuilderFactory resultBuilderFactory = ResultBuilderFactoryImpl.getInstance();
        ResultBuilder resultBuilder = resultBuilderFactory.createResultBuilderForAction(action);
        Result result = null;
        Iterator<Integer> allBucketIndexIterator = getAllBucketIndexIterator();
        while (allBucketIndexIterator.hasNext()) {
            Integer bucketIndex = allBucketIndexIterator.next();
            resultBuilder.addResult(doAction(bucketIndex, action));
        }
        return resultBuilder.build();
    }

    public Iterator<Integer> getAllBucketIndexIterator() {
        // TODO return set of bucketIndex for all buckets.
        return null;
    }

    protected Result doAction(int bucketIndex, Action action) throws BucketServiceException {
        // TODO extract logic to ActionStrategy hierarchy.
        Result result = null;
        if (action.getActionType() != ActionType.READ) {
            ActionStorage.getInstance().addAction(bucketIndex, action);
        }
        // here can return OK, and do other in background.
        try {
            lockBucketIndex(bucketIndex);
//            Shard activeShard = findShard();
            String activeShardId = findActiveShardId(bucketIndex);
            doAction(activeShardId, action);
            // here only for WRITE actions.
            for (String replicaShardId : findReplicaShardIds(bucketIndex)) {
                // may be write to any replicas it is minor error and do some internal stuff without client information. extract logic to ReplicaActionStrategy
                doAction(replicaShardId, action);
            }
            // TODO should we merge any activeShard ReplicaShards results ? seems like shouldn't.

        } finally {
            unlockBucketIndex(bucketIndex);
        }
        return result;
    }

    protected Result doAction(String shardId, Action action) throws BucketServiceException {
        Result result = null;
        Connection connection = null;
        Shard shard = findShard(shardId);
        connection = shard.getConnection();
        try {
            result = action.call(connection);
//            result = filterDataFromNonActiveBuckets(bucket.getBucketIndex(), result);
        } catch (Exception e) {
            onShardError(shardId);
            throw new BucketServiceException(e);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
//        onActionSuccess(bucket, action);
        return result;
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
            if (mapEntityIdToBucketIndex(entry.getKey()) == bucketIndex) {
                resultMap.put(entry.getKey(), entry.getValue());
            }
        }
        result.setEntityIdEntityMap(resultMap);
        return result;
    }

    protected int mapEntityIdToBucketIndex(Long entityId) { // TODO impl.
        return 0;
    }

    /**
     *  to avoid changing bucket to non-actual state (switching active bucket to inactive etc.).
     * @param bucketIndex
     * @return
     */
    protected void unlockBucketIndex(int bucketIndex) {
        // TODO
    }

    private void onShardError(String shardId) throws BucketServiceException {
        removeShard(shardId); // TODO run in separate thread, because bucket switching may be required, and bucket may be locked to changing.
    }

    @Override
    public void createNewReplicaBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException {
        // assumptions:
        //   dstBucket has D state and unused.
        //   srcBucket may A or R.
        // TODO only one worker should be able to move concrete bucket over the all clients (even if on different machines).
        // caller of this method should guarantee it.

        Bucket dstBucket = new Bucket(dstShardId, bucketIndex);
        Bucket srcBucket = new Bucket(srcShardId, bucketIndex);
        try {
            // blockActivationBucket(bucketIndex)
            // check srcBucket,
            //   if R it should be in findReplicaBuckets(bucketIndex) and bucket shouldn't became desynchronized with active bucket.
            //   if A - continue
            //   else - error
            fullSync(srcBucket, dstBucket);
            setBucketState(dstBucket, BucketState.R);
            addReplicaBucket(dstBucket); // NB should works only when no clients using bucketIndex. its ok after fullSync, because fullSync works until no clients using bucketIndex.
            // now we ready to change srcBucket state to A.
            // unblockActivationBucket(bucketIndex)
            // activateBucketWhenNobodyBlockActivationBucket(srcBucket);
            setBucketState(srcBucket, BucketState.A); // TODO if createNewReplicaBucket or moveActiveBucket process is running on same srcBucket, and haven't finished yet, we shouldn't activate dstBucket.
        } catch (BucketServiceException e) {
            // unblockActivationBucket(bucketIndex)
            setBucketState(dstBucket, BucketState.D);
            throw new BucketServiceException(e);
        }
    }

    @Override
    public void moveActiveBucket(String srcShardId, String dstShardId, int bucketIndex) throws BucketServiceException {
        // assumptions:
        //   dstBucket has D state and unused.
        //   srcBucket may A.
        // TODO only one worker should be able to move concrete bucket over the all clients (even if on different machines).
        // caller of this method should guarantee it.

        Bucket dstBucket = new Bucket(dstShardId, bucketIndex);
        Bucket srcBucket = new Bucket(srcShardId, bucketIndex);
        try {
            // blockActivationBucket(bucketIndex)
            fullSync(srcBucket, dstBucket);
            // now we ready to change dstBucket state to A.
            // unblockActivationBucket(bucketIndex)
            // activateBucketWhenNobodyBlockActivationBucket(dstBucket)
            setBucketState(dstBucket, BucketState.A); // TODO if createNewReplicaBucket process is running on same srcBucket, and haven't finished yet, we shouldn't activate dstBucket.

        } catch (BucketServiceException e) {
            // unblockActivationBucket(bucketIndex)
            setBucketState(dstBucket, BucketState.D);
            throw new BucketServiceException(e);
        }
    }


    @Override
    public void addShard(String shardId) throws BucketServiceException {
        // TODO move part of A buckets from existing shards to new one.
        // TODO add R buckets to new shard.
    }

    @Override
    public void removeShard(String shardId) throws BucketServiceException {
        // TODO
        // 1. what if run this method in parallel?
        // 2. what if other public methods will run in parallel with this method?
        moveActiveBucketsFromShard(shardId);
        moveReplicaBucketsFromShard(shardId);
    }

    private void moveActiveBucketsFromShard(String shardId) throws BucketServiceException {
        // TODO
        // 1. what if run this method in parallel?
        // 2. what if other public methods will run in parallel with this method?
        Collection<Bucket> activeBuckets = findActiveBucketsOnShard(shardId);
        for (Bucket activeBucket : activeBuckets) {
            // TODO all code inside for may be run in parallel.
            Bucket newActiveBucket = findReplicaBucketFor(activeBucket);
            if (newActiveBucket != null) {
                // blockActivationBucket(bucketIndex)
                setBucketState(activeBucket, BucketState.D);
                waitUntilSomeoneIsUsingBucket(activeBucket);
                removeReplicaBucket(newActiveBucket);
                // unblockActivationBucket(bucketIndex)
                // activateBucketWhenNobodyBlockActivationBucket(dstBucket)
                setBucketState(newActiveBucket, BucketState.A); // TODO if createNewReplicaBucket or moveActiveBucket process is running on same srcBucket, and haven't finished yet, we shouldn't activate dstBucket.
            } else {
                // if shardId is dead, can skip moveActiveBucket(...).
                Bucket newReplicaBucket = findDBucketForReplicaCreation(activeBucket);
                moveActiveBucket(shardId, newReplicaBucket.getShardId(), activeBucket.getBucketIndex());
            }
        }
    }

    @Override
    public void switchActiveBucket(String dstShardId, int bucketIndex) throws BucketServiceException {
        // TODO if exist at least one sync process, we shouldn't switch.
        // TODO while we are switching, both Buckets shouldn't change them state.
        // TODO after lock Bucket states, check them.
        // blockActivationBucket(bucketIndex)
        Bucket activeBucket = findActiveBucket(bucketIndex);
        if (activeBucket != null) {
            setBucketState(activeBucket, BucketState.D); // TODO if sync process is running on the same bucket, clients will be block until sync finish.
            waitUntilSomeoneIsUsingBucket(activeBucket);
            setBucketState(activeBucket, BucketState.R);
            addReplicaBucket(activeBucket);
        }
        Bucket dstBucket = new Bucket(dstShardId, bucketIndex);
        removeReplicaBucket(dstBucket);
        // unblockActivationBucket(bucketIndex)
        // activateBucketWhenNobodyBlockActivationBucket(dstBucket)
        setBucketState(dstBucket, BucketState.A); // TODO if createNewReplicaBucket or moveActiveBucket process is running on same srcBucket, and haven't finished yet, we shouldn't activate dstBucket.
    }

    private void moveReplicaBucketsFromShard(String shardId) throws BucketServiceException {
        // TODO
        // 1. what if run this method in parallel?
        // 2. what if other public methods will run in parallel with this method?
        Collection<Bucket> replicaBuckets = findReplicaBucketsOnShard(shardId);
        for (Bucket replicaBucket : replicaBuckets) {
            // TODO should we synchronize access to replicaBucket here? whar if running client in this time?
            setBucketState(replicaBucket, BucketState.D);
//            waitUntilSomeoneIsUsingBucket(replicaBucket);
            removeReplicaBucket(replicaBucket);
            if (true) { // todo run in parallel for each bucket.
                Bucket activeBucket = findActiveBucket(replicaBucket.getBucketIndex());
                if (activeBucket != null) {
                    Bucket newReplicaBucket = findDBucketForReplicaCreation(activeBucket);
                    createNewReplicaBucket(activeBucket.getShardId(), newReplicaBucket.getShardId(), activeBucket.getBucketIndex());
                }
            }
        }
    }

    private Bucket findDBucketForReplicaCreation(Bucket activeBucket) {
        return null;  // TODO implement me.
    }

    private Collection<Bucket> findReplicaBucketsOnShard(String shardId) {
        // bucket not only in state R but also synchronized with A
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    private Collection<Bucket> findActiveBucketsOnShard(String shardId) {
        // if on bucketIndex exists at least one replica and (active replica absent or exist on shardId), then add bucket(bucketIndex, shardId) to result,
        // returns Collection of Active buckets on shardId even if shard is died.
        // TODO implement me.
        return null;
    }

    private Bucket findReplicaBucketFor(Bucket bucket) {
        return null; // todo impl.
    }


    private void fullSync(Bucket srcBucket, Bucket dstBucket) throws BucketServiceException {
        prepareDstBucket(dstBucket);
        ActionsQueue actionsQueue = createActionsQueue(srcBucket);
        Action action;
        boolean isAlreadySet = false;
        while ((action = pop(actionsQueue, srcBucket)) != null) { // TODO Is it possible, if we block on pop() forever and never will call setSrcBucketState(D)???
            doAction(dstBucket.getShardId(), action);
            if ((!isAlreadySet) && (actionsQueue.count() < THRESHOLD)) { // TODO if other sync process are running on same srcBucket, and has actionsQueue.count() > THRESHOLD, we shouldn't do that.
                setBucketState(srcBucket, BucketState.D);
                isAlreadySet = true;
            }
        }
    }

    protected void prepareDstBucket(Bucket dstBucket) {
        clearBucket(dstBucket); // need only for fullSync, not for incrementalSync.
        setBucketState(dstBucket, BucketState.P);
    }

    protected void clearBucket(Bucket dstBucket) {
        // todo shard type depended implementation.
    }

    protected ActionsQueueCombo createActionsQueue(Bucket bucket) {
        return new ActionsQueueCombo(bucket);
    }

    private void addReplicaBucket(Bucket newReplicaBucket) {
        // TODO implement me.
    }

    private void removeReplicaBucket(Bucket dstBucket) {
        // TODO implement me.
    }


    protected Action pop(ActionsQueue actionsQueue, Bucket bucket) throws BucketServiceException {
        Action result;
        try {
            while (true) {
                result = actionsQueue.pop();
                if (result == null) {
                    if ((retrieveBucketState(bucket) == BucketState.D) && (retrieveBucketUsageCount(bucket) == 0)) {
                        break;
                    } else {
                        try {
                            wait(TIMEOUT);
                        } catch (InterruptedException e) {
                            logger.error("", e);
                        }
                    }
                }
             }
        } catch (ActionsQueueException e) {
            throw new BucketServiceException(e);
        }
        return result;
    }

    private void waitUntilSomeoneIsUsingBucket(Bucket bucket) throws BucketServiceException {
        while (retrieveBucketUsageCount(bucket) > 0) {
            try {
                wait(TIMEOUT);
            } catch (InterruptedException e) {
                logger.error("", e);
                throw new BucketServiceException(e);
            }
        }
    }

    protected BucketState retrieveBucketState(Bucket bucket) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    protected int retrieveBucketUsageCount(Bucket bucket) {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    protected Bucket findActiveBucket(int bucketIndex) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    protected void setBucketState(Bucket bucket, BucketState bucketState) {
        // TODO
    }

    private String findActiveShardId(int bucketIndex) {
        // TODO implement me.
        return null;
    }

    private Collection<String> findReplicaShardIds(int bucketIndex) {
        // TODO implement me.
        return null;
    }

    /**
     * check shard availability and if not fix it,
     * may be run regularly by timer.
     * @param shardId
     * @throws BucketServiceException
     */
    @Override
    public void checkShard(String shardId) throws BucketServiceException {
        if (!isShardAlive(shardId)) {
            removeShard(shardId);
        }
    }

    private boolean isShardAlive(String shardId) {
        return false;  // TODO implement me.
    }

    private Shard findShard(String shardId) {
        return null;  //TODO implement me.
    }

    /**
     *  to avoid changing bucket to non-actual state (switching active bucket to inactive etc.).
     * @param bucketIndex
     * @return
     */
    protected void lockBucketIndex(int bucketIndex) {
        // TODO
        Bucket bucket = null;
        for (int i = 0; i < MAX_LOCK_COUNT; i++) {
            bucket = findActiveBucket(bucketIndex);
            lockBucket(bucket);
            BucketState bucketState = retrieveBucketState(bucket);
            if (bucketState == BucketState.A) {
                break;
            }
            unlockBucket(bucket);
            //            sleep();//???
        }
//        return bucket;
    }

    protected void lockBucket(Bucket bucket) {
        //To change body of created methods use File | Settings | File Templates.
        // TODO increaseBucketUsageCount()
    }

    protected void unlockBucket(Bucket bucket) {
        //To change body of created methods use File | Settings | File Templates.
        // TODO decreaseBucketUsageCount()
    }

//    protected void onActionSuccess(Bucket bucket, Action action) {
//
//    }
}

package net.thumbtack;

import net.thumbtack.sharding.core.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

public class AdminServiceImpl implements AdminService {
    private static final Logger logger = LoggerFactory.getLogger(AdminServiceImpl.class);

    private static final int THRESHOLD = 10;

    private static final int TIMEOUT = 1000;

    private BucketService bucketService;
    private ShardService shardService;

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
            bucketService.setBucketState(dstBucket, BucketState.R);
            bucketService.addReplicaBucket(dstBucket); // NB should works only when no clients using bucketIndex. its ok after fullSync, because fullSync works until no clients using bucketIndex.
            // now we ready to change srcBucket state to A.
            // unblockActivationBucket(bucketIndex)
            // activateBucketWhenNobodyBlockActivationBucket(srcBucket);
            bucketService.setBucketState(srcBucket, BucketState.A); // TODO if createNewReplicaBucket or moveActiveBucket process is running on same srcBucket, and haven't finished yet, we shouldn't activate dstBucket.
        } catch (BucketServiceException e) {
            // unblockActivationBucket(bucketIndex)
            bucketService.setBucketState(dstBucket, BucketState.D);
            throw new BucketServiceException(e);
        }
    }

    private void fullSync(Bucket srcBucket, Bucket dstBucket) throws BucketServiceException {
        prepareDstBucket(dstBucket);
        ActionsQueue actionsQueue = createActionsQueue(srcBucket);
        Action action;
        boolean isAlreadySet = false;
        while ((action = pop(actionsQueue, srcBucket)) != null) { // TODO Is it possible, if we block on pop() forever and never will call setSrcBucketState(D)???
            try {
                bucketService.doAction(dstBucket.getShardId(), action);
            } catch (BucketServiceException e) {
                removeShard(dstBucket.getShardId()); // TODO run in separate thread, because bucket switching may be required, and bucket may be locked to changing.
                throw new BucketServiceException(e);
            }
            if ((!isAlreadySet) && (actionsQueue.count() < THRESHOLD)) { // TODO if other sync process are running on same srcBucket, and has actionsQueue.count() > THRESHOLD, we shouldn't do that.
                bucketService.setBucketState(srcBucket, BucketState.D);
                isAlreadySet = true;
            }
        }
    }

    @Override
    public void incrementalSync(Bucket bucket, long lastActionId) throws BucketServiceException {
        // TODO only one thread per bucket in same time is allowed.
        long lastAcceptedActionId = bucketService.retrieveLastAcceptedAction(bucket);
        Iterator<Action> actionIterator = ActionStorage.getInstance().findActionsBetween(lastAcceptedActionId, lastActionId);
        while (actionIterator.hasNext()) { // TODO use ActionsQueueInc instead actionIterator?
            Action action = actionIterator.next();
            try {
                bucketService.doAction(bucket.getShardId(), action);
                bucketService.updateLastAcceptedAction(bucket, action.getActionId());
                // TODO when doAction was ok, but updateLastAcceptedAction was fail, action should be undone, otherwise, in future while sync process it will perform again and lead to error (double insert...).
            } catch (BucketServiceException e) {
                removeShard(bucket.getShardId()); // TODO run in separate thread, because bucket switching may be required, and bucket may be locked to changing.
                throw new BucketServiceException(e);
            }
        }
    }

    protected void prepareDstBucket(Bucket dstBucket) {
        clearBucket(dstBucket); // need only for fullSync, not for incrementalSync.
        bucketService.setBucketState(dstBucket, BucketState.P);
    }

    protected void clearBucket(Bucket dstBucket) {
        // todo shard type depended implementation.
    }

    protected ActionsQueueCombo createActionsQueue(Bucket bucket) {
        return new ActionsQueueCombo(bucket);
    }

    protected Action pop(ActionsQueue actionsQueue, Bucket bucket) throws BucketServiceException {
        Action result;
        try {
            while (true) {
                result = actionsQueue.pop();
                if (result == null) {
                    if ((bucketService.retrieveBucketState(bucket) == BucketState.D) && (bucketService.retrieveBucketUsageCount(bucket) == 0)) {
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
            bucketService.setBucketState(dstBucket, BucketState.A); // TODO if createNewReplicaBucket process is running on same srcBucket, and haven't finished yet, we shouldn't activate dstBucket.

        } catch (BucketServiceException e) {
            // unblockActivationBucket(bucketIndex)
            bucketService.setBucketState(dstBucket, BucketState.D);
            throw new BucketServiceException(e);
        }
    }

    private void moveActiveBucketsFromShard(String shardId) throws BucketServiceException {
        // TODO
        // 1. what if run this method in parallel?
        // 2. what if other public methods will run in parallel with this method?
        Collection<Bucket> activeBuckets = bucketService.findActiveBucketsOnShard(shardId);
        for (Bucket activeBucket : activeBuckets) {
            // TODO all code inside for may be run in parallel.
            Bucket newActiveBucket = findReplicaBucketFor(activeBucket);
            if (newActiveBucket != null) {
                // blockActivationBucket(bucketIndex)
                bucketService.setBucketState(activeBucket, BucketState.D);
                bucketService.waitUntilSomeoneIsUsingBucket(activeBucket);
                bucketService.removeReplicaBucket(newActiveBucket);
                // unblockActivationBucket(bucketIndex)
                // activateBucketWhenNobodyBlockActivationBucket(dstBucket)
                bucketService.setBucketState(newActiveBucket, BucketState.A); // TODO if createNewReplicaBucket or moveActiveBucket process is running on same srcBucket, and haven't finished yet, we shouldn't activate dstBucket.
            } else {
                // if shardId is dead, can skip moveActiveBucket(...).
                Bucket newReplicaBucket = findDBucketForReplicaCreation(activeBucket);
                moveActiveBucket(shardId, newReplicaBucket.getShardId(), activeBucket.getBucketIndex());
            }
        }
    }

    private Bucket findReplicaBucketFor(Bucket activeBucket) {
        return null;  // todo impl.
    }

    private Bucket findAnyReplicaBucketFor(Bucket bucket) {
        return null; // todo impl.
    }

    private Bucket findDBucketForReplicaCreation(Bucket activeBucket) {
        return null;  // TODO implement me.
    }

    private Collection<Bucket> findReplicaBucketsOnShard(String shardId) {
        // bucket not only in state R but also synchronized with A
        return null;  //To change body of created methods use File | Settings | File Templates.
    }


    private void moveReplicaBucketsFromShard(String shardId) throws BucketServiceException {
        // TODO
        // 1. what if run this method in parallel?
        // 2. what if other public methods will run in parallel with this method?
        Collection<Bucket> replicaBuckets = findReplicaBucketsOnShard(shardId);
        for (Bucket replicaBucket : replicaBuckets) {
            // TODO should we synchronize access to replicaBucket here? whar if running client in this time?
            bucketService.setBucketState(replicaBucket, BucketState.D);
//            waitUntilSomeoneIsUsingBucket(replicaBucket);
            bucketService.removeReplicaBucket(replicaBucket);
            if (true) { // todo run in parallel for each bucket.
                Bucket activeBucket = bucketService.findActiveBucket(replicaBucket.getBucketIndex());
                if (activeBucket != null) {
                    Bucket newReplicaBucket = findDBucketForReplicaCreation(activeBucket);
                    createNewReplicaBucket(activeBucket.getShardId(), newReplicaBucket.getShardId(), activeBucket.getBucketIndex());
                }
            }
        }
    }

    @Override
    public void removeShard(String shardId) throws BucketServiceException {
        // TODO
        // 1. what if run this method in parallel?
        // 2. what if other public methods will run in parallel with this method?
        moveActiveBucketsFromShard(shardId);
        moveReplicaBucketsFromShard(shardId);
        // TODO now all buckets in D or P states, what to do with P?
        // when all buckets on shard in D(or P?) state it may be removed.
        // remove shard from all clients. [ShardConfig, Shards's Bucket data, etc.]
        shardService.remove(shardId);
    }

    /**
     * check shard availability and if not fix it,
     * may be run regularly by timer.
     * @param shardId
     * @throws BucketServiceException
     */
    @Override
    public void checkShard(String shardId) throws BucketServiceException {
        if (!shardService.isShardAlive(shardId)) {
            removeShard(shardId);
        }
    }
    @Override
    public void switchActiveBucket(String dstShardId, int bucketIndex) throws BucketServiceException {
        // TODO if exist at least one sync process, we shouldn't switch.
        // TODO while we are switching, both Buckets shouldn't change them state.
        // TODO after lock Bucket states, check them.
        // blockActivationBucket(bucketIndex)
        Bucket activeBucket = bucketService.findActiveBucket(bucketIndex);
        if (activeBucket != null) {
            bucketService.setBucketState(activeBucket, BucketState.D); // TODO if sync process is running on the same bucket, clients will be block until sync finish.
            bucketService.waitUntilSomeoneIsUsingBucket(activeBucket);
            bucketService.setBucketState(activeBucket, BucketState.R);
            bucketService.addReplicaBucket(activeBucket);
        }
        Bucket dstBucket = new Bucket(dstShardId, bucketIndex);
        bucketService.removeReplicaBucket(dstBucket);
        // unblockActivationBucket(bucketIndex)
        // activateBucketWhenNobodyBlockActivationBucket(dstBucket)
        bucketService.setBucketState(dstBucket, BucketState.A); // TODO if createNewReplicaBucket or moveActiveBucket process is running on same srcBucket, and haven't finished yet, we shouldn't activate dstBucket.
    }

    @Override
    public void addShard(String shardId, Shard shard) throws BucketServiceException {
        // now, no one client knows about this new shard.
        // prepare shard as clear (all buckets have D-state).
        // publicate shard to all clients. (save shardId, ShardType and ShardConfig to common for all clients place)
        shardService.publicate(shardId, shard);
        // when on shard appear non-D-buckets, all clients have to know about this shard.
        // TODO move part of A buckets from existing shards to new one.
        // TODO add R buckets to new shard.
    }
}

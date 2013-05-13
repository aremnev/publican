package net.thumbtack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketServiceImpl implements BucketService {
    private static final Logger logger = LoggerFactory.getLogger(BucketServiceImpl.class);
    private static final int MAX_LOCK_COUNT = 10;
    private static final int THRESHOLD = 10;


    @Override
    public void doAction(Action action) throws BucketServiceException {
        Bucket bucket = null;
        try {
            bucket = lockBucket(action); // may be more then one Bucket (for example, when WRITE and REPLICA enabled)
            doAction(bucket, action);
            onActionSuccess(bucket, action);
        } finally {
            unlockBucket(bucket);
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
        try {
            prepareDstBucket(dstBucket);
            ActionsQueue actionsQueue = createActionsQueue(new Bucket(srcShardId, bucketIndex));
            Action action;
            boolean seted = false;
            while ((action = actionsQueue.pop()) != null) { // TODO Is it possible, if we block on pop() forever and never will call setSrcBucketState(D)???
                doAction(dstBucket, action);
                if ((!seted) && (actionsQueue.count() < THRESHOLD)) {
                    setBucketState(dstBucket, BucketState.D);
                    seted = true;
                }
            }
            setBucketState(dstBucket, BucketState.A);

        } catch (BucketServiceException e) {
            setBucketState(dstBucket, BucketState.D);
            throw new BucketServiceException(e);
        } catch (ActionsQueueException e) {
            setBucketState(dstBucket, BucketState.D);
            throw new BucketServiceException(e);
        }
    }

    protected Action pop(ActionsQueue actionsQueue, Bucket bucket) throws BucketServiceException {
        Action result;
        try {
            while (true) {
                result = actionsQueue.pop();
                if (result == null) {
                    if ((retrieveBucketState(bucket) == BucketState.D) && (retrieveBucketUsageCount(bucket) == 0)) {
                        result = null;
                        break;
                    } else {
                        try {
                            wait();
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

    protected BucketState retrieveBucketState(Bucket bucket) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    protected int retrieveBucketUsageCount(Bucket bucket) {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    protected Bucket findActiveBucket(int bucketIndex) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    protected void lockBucket(Bucket bucket) {
        //To change body of created methods use File | Settings | File Templates.
        // TODO increaseBucketUsageCount()
    }

    protected void unlockBucket(Bucket bucket) {
        //To change body of created methods use File | Settings | File Templates.
        // TODO decreaseBucketUsageCount()
    }

    protected Bucket lockActiveBucket(int bucketIndex) {
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
        return bucket;
    }

    protected void setBucketState(Bucket bucket, BucketState bucketState) {
        // TODO
    }

    protected void prepareDstBucket(Bucket dstBucket) {
        clearBucket(dstBucket);
        setBucketState(dstBucket, BucketState.P);
    }

    protected void doAction(Bucket bucket, Action action) throws BucketServiceException {
        //To change body of created methods use File | Settings | File Templates.
    }

    protected Bucket lockBucket(Action action) {
        // TODO
        // if READ, get one Bucket (A or any R).
        // else get all A and R Buckets.
        return lockActiveBucketByEntityId(action.getEntityId());
    }

    protected Bucket lockActiveBucketByEntityId(Long entityId) {
        return lockActiveBucket(mapEntityIdToBucketIndex(entityId));
    }

    protected int mapEntityIdToBucketIndex(Long entityId) { // TODO impl.
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    protected void onActionSuccess(Bucket bucket, Action action) {
        ActionStorage.getInstance().addAction(bucket, action);
    }

    protected ActionsQueueCombo createActionsQueue(Bucket bucket) {
        return new ActionsQueueCombo(bucket);
    }

    protected void clearBucket(Bucket dstBucket) {
        // todo shard type depended implementation.
    }
}

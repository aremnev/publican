package net.thumbtack;

abstract public class AbstractBucketMover {
    private static final int THRESHOLD = 10;

    void moveActiveBucket(String srcShardId, String dstShardId, int bucketIndex) {
        // assumptions:
        //   dstBucket has D state and unused.
        //   srcBucket may A.
        // TODO only one worker should be able to move concrete bucket over the all clients (even if on different machines).
        // caller of this method should guarantee it.

        Bucket dstBucket = new Bucket(dstShardId, bucketIndex);
        try {
            prepareDstBucket(dstBucket);
            ActionsQueueCombo actionsQueueCombo = createActionsQueue(new Bucket(srcShardId, bucketIndex));
            Action action;
            boolean seted = false;
            while ((action = actionsQueueCombo.pop()) != null) { // TODO Is it possible, if we block on pop() forever and never will call setSrcBucketState(D)???
                doAction(dstBucket, action);
                if ((!seted) && (actionsQueueCombo.count() < THRESHOLD)) {
                    Utils.setBucketState(dstBucket, BucketState.D);
                    seted = true;
                }
            }
            Utils.setBucketState(dstBucket, BucketState.A);

        } catch (BucketException e) {
            Utils.setBucketState(dstBucket, BucketState.D);
        }
    }

    protected void prepareDstBucket(Bucket dstBucket) {
        clearBucket(dstBucket); // TODO is it required? implementation depends on Storage type (ShardType).
        Utils.setBucketState(dstBucket, BucketState.P);
    }

    abstract protected void clearBucket(Bucket dstBucket);

    private ActionsQueueCombo createActionsQueue(Bucket bucket) {
        ActionsQueueCombo actionsQueueCombo = new ActionsQueueCombo(bucket);
        // TODO
        return actionsQueueCombo;
    }

    private void doAction(Bucket bucket, Action action) throws BucketException {
        //To change body of created methods use File | Settings | File Templates.
    }

    public void doAction(Action action) throws BucketException {
        Bucket bucket = null;
        try {
            bucket = lockBucket(action); // may be more then one Bucket (for example, when WRITE and REPLICA enabled)
            doAction(bucket, action);
            onActionSuccess(bucket, action);
        } finally {
            Utils.unlockBucket(bucket);
        }
    }

    private Bucket lockBucket(Action action) {
        // TODO
        // if READ, get one Bucket (A or any R).
        // else get all A and R Buckets.
        return lockActiveBucketByEntityId(action.getEntityId());
    }

    private Bucket lockActiveBucketByEntityId(Long entityId) {
        return Utils.lockActiveBucket(mapEntityIdToBucketIndex(entityId));
    }

    private int mapEntityIdToBucketIndex(Long entityId) { // TODO impl.
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    private void onActionSuccess(Bucket bucket, Action action) {
        ActionStorage.getInstance().addAction(bucket, action);
    }
}

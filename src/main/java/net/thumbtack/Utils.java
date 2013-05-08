package net.thumbtack;

public class Utils {

    private static final int MAX_LOCK_COUNT = 10;

    public static void setBucketState(Bucket bucket, BucketState bucketState) {
        // TODO
    }

    public static Bucket lockActiveBucket(int bucketIndex) {
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

    public static BucketState retrieveBucketState(Bucket bucket) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public static int retrieveBucketUsageCount(Bucket bucket) {
        return 0;  //To change body of created methods use File | Settings | File Templates.
    }

    public static Bucket findActiveBucket(int bucketIndex) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public static void unlockBucket(Bucket bucket) {
        //To change body of created methods use File | Settings | File Templates.
    }

    public static void lockBucket(Bucket bucket) {
        //To change body of created methods use File | Settings | File Templates.
    }


}

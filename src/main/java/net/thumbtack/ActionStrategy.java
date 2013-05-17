package net.thumbtack;

public interface ActionStrategy {
    Result doAction(int bucketIndex, Action action) throws BucketServiceException; // TODO change exception.
    Result doAction(Action action) throws BucketServiceException; // TODO change exception.
}

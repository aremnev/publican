package net.thumbtack;

public interface ClientService {
    Result doAction(Action action, long entityId, ActionStrategy actionStrategy) throws BucketServiceException;
    Result doReadAction(Action action, ActionStrategy actionStrategy) throws BucketServiceException;
}

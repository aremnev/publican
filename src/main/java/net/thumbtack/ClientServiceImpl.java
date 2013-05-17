package net.thumbtack;

public class ClientServiceImpl implements ClientService {
    private BucketService bucketService;

    @Override
    public Result doAction(Action action, long entityId, ActionStrategy actionStrategy) throws BucketServiceException {
        return actionStrategy.doAction(bucketService.mapEntityIdToBucketIndex(entityId), action);
    }

    @Override
    public Result doReadAction(Action action, ActionStrategy actionStrategy) throws BucketServiceException {
        return actionStrategy.doAction(action);
    }
}

package net.thumbtack;

public class BucketServiceException extends Exception {
    public BucketServiceException(Throwable e) {
        super(e);
    }

    public BucketServiceException(String s) {
        super(s);
    }
}

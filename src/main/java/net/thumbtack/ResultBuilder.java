package net.thumbtack;

public interface ResultBuilder {
    void addResult(Result result);
    Result build();
}

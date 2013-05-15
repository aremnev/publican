package net.thumbtack;

import java.util.LinkedList;
import java.util.List;

public class DefaultResultBuilder implements ResultBuilder {
    private List list = new LinkedList();

    @Override
    public void addResult(Result result) {
        list.add(result.getResult());
    }

    @Override
    public Result build() {
        Result result = new Result();
        result.setResult(list);
        return result;
    }
}

package net.thumbtack;


public class ResultBuilderFactoryImpl implements ResultBuilderFactory {
    private static ResultBuilderFactory instance = new ResultBuilderFactoryImpl();

    private ResultBuilderFactoryImpl() {

    }

    public static ResultBuilderFactory getInstance() {
        return instance;
    }

    @Override
    public ResultBuilder createResultBuilderForAction(Action action) {
        return new DefaultResultBuilder();
    }
}

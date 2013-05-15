package net.thumbtack;

import net.thumbtack.Action;
import net.thumbtack.ResultBuilder;


public interface ResultBuilderFactory {
    ResultBuilder createResultBuilderForAction(Action action);
}

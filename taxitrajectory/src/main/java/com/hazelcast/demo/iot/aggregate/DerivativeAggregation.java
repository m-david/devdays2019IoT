package com.hazelcast.demo.iot.aggregate;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.BiFunctionEx;

import java.io.Serializable;

public class DerivativeAggregation {

    /**
     * Aggregate operation to calculate a derivative in rolling aggregation.
     * The
     */
    public static <T, OUT> AggregateOperation1<T, DerivativeAccumulator<T, OUT>, OUT> derivative(

            BiFunctionEx<T, T, OUT> differenceFn
    ) {
        return AggregateOperation.withCreate(DerivativeAccumulator<T, OUT>::new)
                .<T>andAccumulate((acc, item) -> {
                    if (acc.lastItem == null) {
                        acc.lastItem = item;
                    }
                    acc.lastDifference = differenceFn.apply(item, acc.lastItem);
//                    acc.totalDifference = (OUT) Double.valueOf((Double) acc.lastDifference + (Double) acc.lastDifference);
                    acc.lastItem = item;
                })
                .andExportFinish(acc -> acc.lastDifference);
    }

    private static final class DerivativeAccumulator<T, OUT> implements Serializable {
        T lastItem;
        OUT lastDifference;
//        OUT totalDifference;
    }
}
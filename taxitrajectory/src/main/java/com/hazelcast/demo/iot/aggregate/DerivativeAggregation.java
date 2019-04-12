package com.hazelcast.demo.iot.aggregate;

import com.hazelcast.demo.iot.data.TaxiData;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.BiFunctionEx;

import java.io.Serializable;
import java.util.Map;


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
                    acc.totalDifference = acc.totalDifference + ((Double) acc.lastDifference);

                    TaxiData lastTaxiData = (TaxiData) acc.lastItem;
                    TaxiData thisTaxiData = (TaxiData) item;

                    long deltaMsec = thisTaxiData.getTimestamp( ).getTime( ) -
                            lastTaxiData.getTimestamp( ).getTime( );

                    double deltaHours = ((double) deltaMsec) / (1000 * 60 * 60);

                    acc.speed = ((Double) acc.lastDifference) / deltaHours;


                    acc.lastItem = item;
                })
                .andExportFinish(acc -> (OUT) acc.speed);
    }

    private static final class DerivativeAccumulator<T, OUT> implements Serializable {
        T lastItem;
        OUT lastDifference;
        Double totalDifference = 0.0;
        Double speed = 0.0;
    }
}

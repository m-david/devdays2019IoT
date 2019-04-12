package com.hazelcast.demo.iot;

import com.hazelcast.demo.iot.aggregate.DerivativeAggregation;
import com.hazelcast.demo.iot.data.EarthTools;
import com.hazelcast.demo.iot.data.TaxiData;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;


public class TaxiSource {

    private static final String MAP_NAME = "devdaysIOTmap";

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetClient();
        Pipeline p = buildPipeline();

        try {
            Job job = jet.newJob(p);

            job.join();
        }
        finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        long lagTime = 0L; //12 * 60 * 1000L;
        p.drawFrom(Sources.<Integer, TaxiData>mapJournal(MAP_NAME, START_FROM_OLDEST))
            .withTimestamps(entry -> entry.getValue().getTimestamp().getTime(), lagTime)
            .map(entry -> entry.getValue())
            .groupingKey(TaxiData::getID)
            .rollingAggregate(
                    DerivativeAggregation.derivative(
                            (taxiData, lastTaxiData) ->
                                    EarthTools.distanceBetween(taxiData.getLocation(), lastTaxiData.getLocation())

                    ))
            .drainTo(Sinks.logger());

        return p;
    }

}

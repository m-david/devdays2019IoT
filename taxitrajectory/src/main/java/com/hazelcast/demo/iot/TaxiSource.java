package com.hazelcast.demo.iot;

import com.hazelcast.core.IMap;
import com.hazelcast.demo.iot.aggregate.DerivativeAggregation;
import com.hazelcast.demo.iot.data.EarthTools;
import com.hazelcast.demo.iot.data.TaxiData;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import static com.hazelcast.jet.function.PredicateEx.alwaysTrue;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;


public class TaxiSource {

    private static final String MAP_NAME = "devdaysIOTmap";

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetClient();
        Pipeline p = buildPipeline(jet);

        try {
            Job job = jet.newJob(p);

            job.join();
        }
        finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline(JetInstance jet) {
        Pipeline p = Pipeline.create();
        long lagTime = 0L; //12 * 60 * 1000L;
        IMap<Integer, TaxiData> sourceMap = jet.getMap(MAP_NAME);
        p.drawFrom(Sources.mapJournal(sourceMap, alwaysTrue(), Util.mapEventNewValue(), START_FROM_OLDEST))
            .withTimestamps(entry -> entry.getTimestamp().getTime(), lagTime)
            .groupingKey(TaxiData::getID)
            .rollingAggregate(
                    DerivativeAggregation.<TaxiData, Data>derivative(
                            (taxiData, lastTaxiData) -> {
                                double distance = EarthTools.distanceBetween(taxiData.getLocation(), lastTaxiData.getLocation());
                                long deltaMsec = taxiData.getTimestamp( ).getTime( ) -
                                        lastTaxiData.getTimestamp( ).getTime( );

                                double deltaHours = ((double) deltaMsec) / (1000 * 60 * 60);
                                Data res = new Data();
                                res.speed = distance / deltaHours;
                                res.time = taxiData.getTimestamp().getTime();
                                res.taxiId = taxiData.getID();
                                return res;
                            }

                    ))
                .filter(entry -> entry.getValue().speed > 1.0 && entry.getValue().speed < 100.0)
                .map(Map.Entry::getValue)
            .drainTo(Sinks.logger());

        return p;
    }

    static final class Data {
        String taxiId;
        double speed;
        long time;

        @Override
        public String toString() {
            return "Data{" +
                    "taxiId='" + taxiId + '\'' +
                    ", speed=" + speed +
                    ", time=" + Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime() +
                    '}';
        }
    }

}

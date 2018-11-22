package com.rackspacecloud.metrics.rollup.domain;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RolledUp {
    public String key;
    public long start;
    public long end;

    public Map<String, Long> ivalues;
    public Map<String, Double> fvalues;

    // InfluxDB tags
    public AccountType accountType;
    public String account;
    public String device;
    public String deviceLabel;
    public Map<String, String> deviceMetadata;
    public MonitoringSystem monitoringSystem;
    public Map<String, String> systemMetadata;
    public String collectionLabel;
    public String collectionTarget;
    public Map<String, String> collectionMetadata;

    public Map<String, String> units;


    // TODO: temp thing - just to validate rollup
    public Map<String, String> iValuesForRollup;
    public Map<String, String> fValuesForRollup;


    public RolledUp(){
        this.ivalues = new HashMap<>();
        this.fvalues = new HashMap<>();

        this.deviceMetadata = new HashMap<>();
        this.systemMetadata = new HashMap<>();
        this.collectionMetadata = new HashMap<>();

        this.units = new HashMap<>();

        //TODO: delete it after test
        iValuesForRollup = new HashMap<>();
        fValuesForRollup = new HashMap<>();
    }

    public RolledUp(String key, long start, long end, Metric metric){
        this();
        this.key = key;
        this.start = start;
        this.end = end;

        metric.iValuesForRollup.forEach((k, v) -> {
            Long sum = 0L;

            for(int i = 0; i < v.size(); i++){
                sum += v.get(i);
            }
            this.ivalues.put(k, sum/v.size()); // Calculate mean
        });

//        metric.fvalues.forEach((k, v) -> this.fvalues.put(k, v));

        populateMetricData(metric);
    }

    private void populateMetricData(Metric metric){
        this.account = metric.account;
        this.accountType = metric.accountType;
        this.device = metric.device;
        this.deviceLabel = metric.deviceLabel;
        metric.deviceMetadata.forEach((k, v) -> this.deviceMetadata.put(k, v));
        this.monitoringSystem = metric.monitoringSystem;
        metric.systemMetadata.forEach((k, v) -> this.systemMetadata.put(k, v));
        this.collectionLabel = metric.collectionLabel;
        this.collectionTarget = metric.collectionTarget;
        metric.collectionMetadata.forEach((k, v) -> this.collectionMetadata.put(k, v));
        metric.units.forEach((k, v) -> this.units.put(k, v));

        if(metric.iValuesForRollup != null)
            metric.iValuesForRollup.forEach((mKey, mVal) -> {
                String listedVals = "";
                for(int i = 0; i < mVal.size(); i++){
                    listedVals += mVal.get(i) + ";";
                }
                this.iValuesForRollup.put(mKey, listedVals);
            });
    }

    ////        public Map<String, RollupBucket<Long>> ivalues;
////        public Map<String, RollupBucket<Double>> fvalues;
//
////        public static class RollupBucket<T> {
////            public T min;
////            public T mean;
////            public T max;
////        }
}

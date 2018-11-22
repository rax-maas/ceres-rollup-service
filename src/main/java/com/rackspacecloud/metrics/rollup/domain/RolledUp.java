package com.rackspacecloud.metrics.rollup.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains rolled-up data
 */
public class RolledUp {

    private static final Logger LOGGER = LoggerFactory.getLogger(RolledUp.class);

    public String key;  // Rolled-up key
    public long start;  // Start timestamp for the rolled-up data
    public long end;    // End timestamp for the rolled-up data

    public Map<String, RollupBucket<Long>> ivalues;   // Keeps rolled up ivalue
    public Map<String, RollupBucket<Double>> fvalues; // Keeps rolled up fvalue

    public static class RollupBucket<T> {
        public T min;
        public T mean;
        public T max;
    }

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

    public Map<String, String> iValuesForRollup; // Keeps all of the ivalues that rolls up
    public Map<String, String> fValuesForRollup; // Keeps all of the fvalues that rolls up


    /**
     * Initializes all of the maps
     */
    public RolledUp(){
        this.ivalues = new HashMap<>();
        this.fvalues = new HashMap<>();

        this.deviceMetadata = new HashMap<>();
        this.systemMetadata = new HashMap<>();
        this.collectionMetadata = new HashMap<>();

        this.units = new HashMap<>();

        iValuesForRollup = new HashMap<>();
        fValuesForRollup = new HashMap<>();
    }

    /**
     * Creates the rolled-up message
     * @param key
     * @param start
     * @param end
     * @param metric
     */
    public RolledUp(String key, long start, long end, Metric metric){
        this();
        this.key = key;
        this.start = start;
        this.end = end;

        calculateMean(metric);
        populateMetricData(metric);
    }

    private void calculateMean(Metric metric) {
        calculateMeanForIValues(metric);
        calculateMeanForFValues(metric);
    }

    private void calculateMeanForIValues(Metric metric) {
        // Process all collected iValues
        metric.iValuesForRollup.forEach((k, v) -> {
            int dataPointCount = v.size();

            if(dataPointCount == 0) {
                LOGGER.error("There is no data to rollup for key [{}].", k);
            }
            else {
                RollupBucket<Long> rollupBucket = new RollupBucket<>();
                rollupBucket.min = v.get(0);
                rollupBucket.max = rollupBucket.min;

                Long sum = 0L;

                for (int i = 1; i < dataPointCount; i++) {
                    Long currentValue = v.get(i);
                    if(currentValue < rollupBucket.min) rollupBucket.min = currentValue;
                    if(currentValue > rollupBucket.max) rollupBucket.max = currentValue;
                    sum += currentValue;
                }
                rollupBucket.mean = sum/dataPointCount;
                this.ivalues.put(k, rollupBucket);
            }
        });
    }

    private void calculateMeanForFValues(Metric metric) {
        // Process all collected fValues
        metric.fValuesForRollup.forEach((k, v) -> {
            int dataPointCount = v.size();

            if(dataPointCount == 0) {
                LOGGER.error("There is no data to rollup for key [{}].", k);
            }
            else {
                RollupBucket<Double> rollupBucket = new RollupBucket<>();
                rollupBucket.min = v.get(0);
                rollupBucket.max = rollupBucket.min;

                Double sum = 0D;

                for (int i = 1; i < dataPointCount; i++) {
                    Double currentValue = v.get(i);
                    if(currentValue < rollupBucket.min) rollupBucket.min = currentValue;
                    if(currentValue > rollupBucket.max) rollupBucket.max = currentValue;
                    sum += currentValue;
                }
                rollupBucket.mean = sum/dataPointCount;
                this.fvalues.put(k, rollupBucket);
            }
        });
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

        if(metric.iValuesForRollup != null) {
            metric.iValuesForRollup.forEach((mKey, mVal) -> {
                String listedVals = "";
                for (int i = 0; i < mVal.size(); i++) {
                    listedVals += mVal.get(i) + ";";
                }
                this.iValuesForRollup.put(mKey, listedVals);
            });
        }
    }
}

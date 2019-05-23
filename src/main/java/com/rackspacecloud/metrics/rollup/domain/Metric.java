package com.rackspacecloud.metrics.rollup.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class Metric implements IReducer<Metric> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);

    public String rollupKey;

    // InfluxDB tags
    String accountType;
    String account;
    String device;
    String deviceLabel;
    ConcurrentMap<String, String> deviceMetadata;
    String monitoringSystem;
    ConcurrentMap<String, String> systemMetadata;
    String collectionName;
    String collectionLabel;
    String collectionTarget;
    ConcurrentMap<String, String> collectionMetadata;

    // InfluxDB fields
    public ConcurrentMap<String, Long> ivalues;
    public ConcurrentMap<String, Double> fvalues;
    public ConcurrentMap<String, String> svalues;
    public ConcurrentMap<String, String> units;

    @JsonIgnore
    private ConcurrentMap<String, SynchronizedDescriptiveStatistics>  statsOnIValues;

    @JsonIgnore
    private ConcurrentMap<String, SynchronizedDescriptiveStatistics> statsOnFValues;

    public ConcurrentMap<String, RollupBucket<Double>> rolledUpIValues;   // Keeps rolled up ivalue
    public ConcurrentMap<String, RollupBucket<Double>> rolledUpFValues; // Keeps rolled up fvalue

    public Metric() {
        statsOnIValues = new ConcurrentHashMap<>();
        statsOnFValues = new ConcurrentHashMap<>();
    }

    public ConcurrentMap<String, RollupBucket<Double>> getExistingRolledUpIValues(){
        return rolledUpIValues;
    }

    public ConcurrentMap<String, RollupBucket<Double>> getExistingRolledUpFValues(){
        return rolledUpFValues;
    }

    public ConcurrentMap<String, RollupBucket<Double>> getRolledUpIValues(){
        return getRolledUpValuesFromStats(statsOnIValues);
    }

    public ConcurrentMap<String, RollupBucket<Double>> getRolledUpFValues(){
        return getRolledUpValuesFromStats(statsOnFValues);
    }

    @Data
    public static class RollupBucket<T extends Number> {
        private T min;
        private T max;
        private T mean;

        private T sum;
        private long count;
    }

    private ConcurrentMap<String, RollupBucket<Double>> getRolledUpValuesFromStats(
            ConcurrentMap<String, SynchronizedDescriptiveStatistics> statsOnValues) {

        ConcurrentHashMap<String, RollupBucket<Double>> rolledUpValues = new ConcurrentHashMap<>();
        statsOnValues.forEach((k, v) -> {
            RollupBucket<Double> rollupBucket = new RollupBucket<>();
            rollupBucket.min = v.getMin();
            rollupBucket.max = v.getMax();
            rollupBucket.mean = v.getMean();

            rolledUpValues.put(k, rollupBucket);
        });

        return rolledUpValues;
    }

    /**
     * Define the rollup-key that is used to aggregate the data.
     * @return rollup-key
     */
    @Override
    public String getRollupKey() {
        this.rollupKey = String.join(".",
                accountType, account, monitoringSystem, collectionName, device, deviceLabel, collectionLabel);

        return this.rollupKey;
    }

    @Override
    public Metric reduce(Metric newValue) {
        reduceIValuesStats(newValue.getIvalues());
        reduceFValuesStats(newValue.getFvalues());
        return this;
    }

    private void reduceIValuesStats(ConcurrentMap<String, Long> newIValues) {
        reduceIValues(newIValues, statsOnIValues);
    }

    private void reduceIValues(
            ConcurrentMap<String, Long> newValues,
            ConcurrentMap<String, SynchronizedDescriptiveStatistics> statsOnValues) {

        newValues.forEach((k, v) -> statsOnValues.compute(k, (key, value) -> {
            if(value == null) {
                value = new SynchronizedDescriptiveStatistics();
            }
            value.addValue(v.doubleValue());
            return value;
        }));
    }

    private void reduceFValuesStats(ConcurrentMap<String, Double> newFValues) {
        reduceFValues(newFValues, statsOnFValues);
    }

    private void reduceFValues(
            ConcurrentMap<String, Double> newValues,
            ConcurrentMap<String, SynchronizedDescriptiveStatistics> statsOnValues) {

        newValues.forEach((k, v) -> statsOnValues.compute(k, (key, value) -> {
            if(value == null) {
                value = new SynchronizedDescriptiveStatistics();
            }
            value.addValue(v);
            return value;
        }));
    }
}

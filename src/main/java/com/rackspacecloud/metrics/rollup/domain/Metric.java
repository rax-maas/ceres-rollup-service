package com.rackspacecloud.metrics.rollup.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class Metric implements IReducer<Metric> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Metric.class);

    public String rollupKey;

//    public Map<String, List<Long>> iValuesForRollup;
//    public Map<String, List<Double>> fValuesForRollup;

    // InfluxDB tags
    String accountType;
    String account;
    String device;
    String deviceLabel;
    Map<String, String> deviceMetadata;
    String monitoringSystem;
    Map<String, String> systemMetadata;
    String collectionName;
    String collectionLabel;
    String collectionTarget;
    Map<String, String> collectionMetadata;

    // InfluxDB fields
    public Map<String, Long> ivalues;
    public Map<String, Double> fvalues;
    public Map<String, String> svalues;
    public Map<String, String> units;



    public Map<String, RollupBucket<Long>> rolledUpIValues;   // Keeps rolled up ivalue
    public Map<String, RollupBucket<Double>> rolledUpFValues; // Keeps rolled up fvalue

    @Data
    public static class RollupBucket<T extends Number> {
        private T min;
        private T max;
        private T mean;

        private T sum;
        private long count;
    }

    /**
     * Define the rollup-key that is used to aggregate the data.
     * @return rollup-key
     */
    @Override
    public String getRollupKey() {
        String[] rollupKeyTags = new String[] {
                accountType, account, monitoringSystem, collectionName,
                device, deviceLabel, collectionLabel
        };

        this.rollupKey = String.join(".", rollupKeyTags);

        // TODO: Check what else needs to be part of the rollup key

        return this.rollupKey;
    }

    @Override
    public Metric reduce(Metric newValue) {
        reduceIValues(newValue.getIvalues());
        reduceFValues(newValue.getFvalues());
        return this;
    }

    private void reduceIValues(Map<String, Long> newIValues) {
        if(rolledUpIValues == null) {
            rolledUpIValues = new HashMap<>();
        }

        newIValues.forEach((k, v) -> {
            if(!rolledUpIValues.containsKey(k)) { // Initialize rollup bucket for given key
                RollupBucket<Long> rollupBucket = new RollupBucket<>();
                rollupBucket.min = v;
                rollupBucket.max = v;
                rollupBucket.sum = v;
                rollupBucket.count = 1;
                rollupBucket.mean = rollupBucket.sum/rollupBucket.count;

                LOGGER.debug("Initializing rollup bucket for key [{}] " +
                                "with values [min:{};max:{};sum:{};count:{};mean:{}]",
                        k, rollupBucket.min, rollupBucket.max, rollupBucket.sum, rollupBucket.count, rollupBucket.mean);
                rolledUpIValues.put(k, rollupBucket);
            }
            else { // Process new iValues
                RollupBucket<Long> rollupBucket = rolledUpIValues.get(k);
                if(v < rollupBucket.min) rollupBucket.min = v;
                if(v > rollupBucket.max) rollupBucket.max = v;
                rollupBucket.sum += v;
                rollupBucket.count++;
                rollupBucket.mean = rollupBucket.sum/rollupBucket.count;

                LOGGER.debug("Processed rollup bucket for key [{}] " +
                                "with values [min:{};max:{};sum:{};count:{};mean:{}]",
                        k, rollupBucket.min, rollupBucket.max, rollupBucket.sum, rollupBucket.count, rollupBucket.mean);
                rolledUpIValues.put(k, rollupBucket);
            }
        });
    }

    private void reduceFValues(Map<String, Double> newFValues) {
        if(rolledUpFValues == null) {
            rolledUpFValues = new HashMap<>();
        }

        newFValues.forEach((k, v) -> {
            if(!rolledUpFValues.containsKey(k)) { // Initialize rollup bucket for given key
                RollupBucket<Double> rollupBucket = new RollupBucket<>();
                rollupBucket.min = v;
                rollupBucket.max = v;
                rollupBucket.sum = v;
                rollupBucket.count = 1;
                rollupBucket.mean = rollupBucket.sum/rollupBucket.count;

                LOGGER.debug("Initializing rollup bucket for key [{}] " +
                                "with values [min:{};max:{};sum:{};count:{};mean:{}]",
                        k, rollupBucket.min, rollupBucket.max, rollupBucket.sum, rollupBucket.count, rollupBucket.mean);
                rolledUpFValues.put(k, rollupBucket);
            }
            else { // Process new fValues
                RollupBucket<Double> rollupBucket = rolledUpFValues.get(k);
                if(v < rollupBucket.min) rollupBucket.min = v;
                if(v > rollupBucket.max) rollupBucket.max = v;
                rollupBucket.sum += v;
                rollupBucket.count++;
                rollupBucket.mean = rollupBucket.sum/rollupBucket.count;

                LOGGER.debug("Processed rollup bucket for key [{}] " +
                                "with values [min:{};max:{};sum:{};count:{};mean:{}]",
                        k, rollupBucket.min, rollupBucket.max, rollupBucket.sum, rollupBucket.count, rollupBucket.mean);
                rolledUpFValues.put(k, rollupBucket);
            }
        });
    }
}

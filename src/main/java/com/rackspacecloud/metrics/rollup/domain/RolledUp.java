package com.rackspacecloud.metrics.rollup.domain;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains rolled-up data
 */
@Data
public class RolledUp {

    private static final Logger LOGGER = LoggerFactory.getLogger(RolledUp.class);

    public String key;  // Rolled-up key
    public long start;  // Start timestamp for the rolled-up data
    public long end;    // End timestamp for the rolled-up data

    public Map<String, Metric.RollupBucket<Double>> ivalues;   // Keeps rolled up ivalue
    public Map<String, Metric.RollupBucket<Double>> fvalues; // Keeps rolled up fvalue

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

    public Map<String, String> units;

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

        populateMetricData(metric);
    }

    private void populateMetricData(Metric metric) {
        this.account = metric.account;
        this.accountType = metric.accountType;
        this.device = metric.device;
        this.deviceLabel = metric.deviceLabel;
        metric.deviceMetadata.forEach((k, v) -> this.deviceMetadata.put(k, v));
        this.monitoringSystem = metric.monitoringSystem;
        metric.systemMetadata.forEach((k, v) -> this.systemMetadata.put(k, v));
        this.collectionName = metric.collectionName;
        this.collectionLabel = metric.collectionLabel;
        this.collectionTarget = metric.collectionTarget;
        metric.collectionMetadata.forEach((k, v) -> this.collectionMetadata.put(k, v));
        metric.units.forEach((k, v) -> this.units.put(k, v));

        this.ivalues = metric.getExistingRolledUpIValues();
        this.fvalues = metric.getExistingRolledUpFValues();
    }
}

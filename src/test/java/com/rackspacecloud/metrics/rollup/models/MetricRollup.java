package com.rackspacecloud.metrics.rollup.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetricRollup {
    @JsonProperty("key")
    String key;

    @JsonProperty("start")
    long start;

    @JsonProperty("end")
    long end;

    @JsonProperty("accountType")
    String accountType;

    @JsonProperty("account")
    String account;

    @JsonProperty("device")
    String device;

    @JsonProperty("deviceLabel")
    String deviceLabel;

    @JsonProperty("monitoringSystem")
    String monitoringSystem;

    @JsonProperty("collectionLabel")
    String collectionLabel;

    @JsonProperty("collectionTarget")
    String collectionTarget;

    @JsonProperty("units")
    Map<String, String> units;

    @JsonProperty("collectionMetadata")
    Map<String, String> collectionMetadata;

    @JsonProperty("systemMetadata")
    Map<String, String> systemMetadata;

    @JsonProperty("deviceMetadata")
    Map<String, String> deviceMetadata;

    @JsonProperty("ivalues")
    Map<String, RollupBucket<Long>> ivalues;

    @JsonProperty("fvalues")
    Map<String, RollupBucket<Double>> fvalues;

    @Data
    public static class RollupBucket<T> {
        T min;
        T max;
        T mean;
    }
}

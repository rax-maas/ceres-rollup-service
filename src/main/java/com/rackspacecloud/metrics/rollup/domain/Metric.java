package com.rackspacecloud.metrics.rollup.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Metric implements IReducer<Metric> {
    public String rollupKey;

    public Map<String, List<Long>> iValuesForRollup;
    public Map<String, List<Double>> fValuesForRollup;

    public String timestamp;

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

    // InfluxDB fields
    public Map<String, Long> ivalues;
    public Map<String, Double> fvalues;
    public Map<String, String> svalues;
    public Map<String, String> units;

    /**
     * Define the rollup-key that is used to aggregate the data.
     * @return rollup-key
     */
    @Override
    public String getRollupKey(){
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s", getSystemMetadata()));
        this.rollupKey = sb.toString();

        // TODO: Check what else needs to be part of the rollup key

        return rollupKey;
    }

    private String getSystemMetadata(){
        return String.format("%s.%s.%s.%s.%s",
                systemMetadata.get("tenantId"),
                systemMetadata.get("checkType"),
                systemMetadata.get("accountId"),
                systemMetadata.get("entityId"),
                systemMetadata.get("checkId")
        );
    }

    @Override
    public Metric reduce(Metric newValue) {
        reduceIValues(newValue);
        reduceFValues(newValue);
        return this;
    }

    private void reduceIValues(Metric newValue) {
        if(iValuesForRollup == null) {
            iValuesForRollup = new HashMap<>();
            ivalues.forEach((k,v) -> {
                List<Long> valueList = new ArrayList<>();
                valueList.add(v);
                iValuesForRollup.put(k, valueList);
            });
        }

        Map<String, Long> newIValues = newValue.ivalues;

        for(String iKey : newIValues.keySet()){
            if(this.iValuesForRollup.containsKey(iKey)){
                List<Long> iValuesList = this.iValuesForRollup.get(iKey);
                iValuesList.add(newIValues.get(iKey));
            }
            else {
                List<Long> valueList = new ArrayList<>();
                valueList.add(newIValues.get(iKey));
                this.iValuesForRollup.put(iKey, valueList);
            }
        }
    }

    private void reduceFValues(Metric newValue) {
        if(fValuesForRollup == null) {
            fValuesForRollup = new HashMap<>();
            fvalues.forEach((k,v) -> {
                List<Double> valueList = new ArrayList<>();
                valueList.add(v);
                fValuesForRollup.put(k, valueList);
            });
        }

        Map<String, Double> newFValues = newValue.fvalues;

        for(String fKey : newFValues.keySet()){
            if(this.fValuesForRollup.containsKey(fKey)){
                List<Double> fValuesList = this.fValuesForRollup.get(fKey);
                fValuesList.add(newFValues.get(fKey));
            }
            else {
                List<Double> valueList = new ArrayList<>();
                valueList.add(newFValues.get(fKey));
                this.fValuesForRollup.put(fKey, valueList);
            }
        }
    }
}

package com.rackspacecloud.metrics.rollup.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Metric implements IReducer<Metric> {
    public String rollupKey; // TODO: Can I move it somewhere else?

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
        if(iValuesForRollup == null) {
            iValuesForRollup = new HashMap<>();
            ivalues.forEach((k,v) -> {
                List<Long> valueList = new ArrayList<>();
                valueList.add(v);
                iValuesForRollup.put(k, valueList);
            });
        }

        // TODO: do the same thing as done for iValuesForRollup
        if(fValuesForRollup == null) fValuesForRollup = new HashMap<>();

        Map<String, Long> newIValues = newValue.ivalues;
        Map<String, Double> newFValues = newValue.fvalues;

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

        //TODO: Visit below code...
        for(String fKey : newFValues.keySet()){
            if(this.fvalues.containsKey(fKey)){
                Double oldVal = this.fvalues.get(fKey);
                this.fvalues.put(fKey, oldVal + newFValues.get(fKey));
            }
            else {
                this.fvalues.put(fKey, newFValues.get(fKey));
            }
        }

        return this;
    }
}

package com.rackspacecloud.metrics.rollup.producer;

import com.rackspace.maas.model.AccountType;
import com.rackspace.maas.model.Metric;
import com.rackspace.maas.model.MonitoringSystem;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MockMetricHelper {

    public static Metric getValidMetric(int i, String tenantId, boolean wantIValues){
        Metric metric = new Metric();

        metric.setAccount("1234567");
        metric.setAccountType(AccountType.CORE);
        metric.setDevice((1000 + i) + "");
        metric.setDeviceLabel("dummy-device-label-" + i);
        metric.setDeviceMetadata(new HashMap<>());
        metric.setMonitoringSystem(MonitoringSystem.MAAS);

        Map<String, String> systemMetadata = new HashMap<>();
        systemMetadata.put("checkType", "agent.filesystem");
        systemMetadata.put("tenantId", tenantId);
        systemMetadata.put("accountId", "dummy-account-id-" + i);
        systemMetadata.put("entityId", "dummy-entity-id-" + i);
        systemMetadata.put("checkId", "dummy-check-id-" + i);
        systemMetadata.put("monitoringZone", "");
        metric.setSystemMetadata(systemMetadata);

        metric.setCollectionLabel("dummy-collection-label");
        metric.setCollectionTarget("");

        Map<String, String> collectionMetadata = new HashMap<>();
        collectionMetadata.put("rpc_maas_version", "1.7.7");
        collectionMetadata.put("rpc_maas_deploy_date", "2018-10-04");
        collectionMetadata.put("rpc_check_category", "host");
        collectionMetadata.put("product", "osa");
        collectionMetadata.put("osa_version", "14.2.4");
        collectionMetadata.put("rpc_env_identifier", "as-c");
        metric.setCollectionMetadata(collectionMetadata);

        Map<String, Long> iValues = new HashMap<>();

        if(wantIValues) iValues = getIValues();

        metric.setIvalues(iValues);

        metric.setFvalues(new HashMap<>());
        metric.setSvalues(new HashMap<>());

        Map<String, String> units = new HashMap<>();
        units.put("filesystem.free_files", "free_files");
        units.put("filesystem.files", "files");
        units.put("filesystem.total", "KILOBYTES");
        units.put("filesystem.free", "KILOBYTES");
        units.put("filesystem.avail", "KILOBYTES");
        units.put("filesystem.used", "KILOBYTES");

        metric.setUnits(units);
        metric.setTimestamp(Instant.now().toString());

        return metric;
    }

    private static Map<String, Long> getIValues() {
        Map<String, Long> iValues = new HashMap<>();
        iValues.put("filesystem.total", getNextLongValue());
        iValues.put("filesystem.free", getNextLongValue());
        iValues.put("filesystem.free_files", getNextLongValue());
        iValues.put("filesystem.avail", getNextLongValue());
        iValues.put("filesystem.files", getNextLongValue());
        iValues.put("filesystem.used", getNextLongValue());
        return iValues;
    }

    private static long getNextLongValue() {
        return ThreadLocalRandom.current().nextLong(1000L, 50_000L);
    }

}

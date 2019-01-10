package com.rackspacecloud.metrics.rollup.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Arrays;
import java.util.List;

@ConfigurationProperties("kafka")
@Data
public class KafkaConfigurationProperties {
    private List<String> servers;

    public void setServers(String servers){
        this.servers = Arrays.asList(servers.split(";"));
    }

    private Properties properties;
    private Ssl ssl;
    private Consumer consumer;
    private Topics topics;
    private Streams streams;

    @Data
    public static class Consumer{
        private String autoOffsetResetConfig;
    }

    @Data
    public static class Ssl {
        private String truststoreLocation;
        private String truststorePassword;
        private String keystoreLocation;
        private String keystorePassword;
        private String keyPassword;
        private String endpointIdentificationAlgorithm;
    }

    @Data
    public static class Properties{
        private String securityProtocol;
    }

    @Data
    public static class Topics {
        private String in;
        private String outLevel1;
        private String outLevel2;
        private String outLevel3;
        private String outLevel4;
        private String outLevel5;
    }

    @Data
    public static class Streams {
        private String applicationId;
        private String stateDirConfig;
        private int requestTimeoutMs;
        private Aggregation aggregation;

        @Data
        public static class Aggregation {
            private int lateArrivalInMinutes;
            private int windowSizeInMinutesLevel1;
            private int windowSizeInMinutesLevel2;
            private int windowSizeInMinutesLevel3;
            private int windowSizeInMinutesLevel4;
            private int windowSizeInMinutesLevel5;
        }
    }
}

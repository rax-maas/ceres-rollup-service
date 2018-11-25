package com.rackspacecloud.metrics.rollup.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Arrays;
import java.util.List;

@ConfigurationProperties("kafka")
public class KafkaConfigurationProperties {
    private List<String> servers;

    public List<String> getServers(){
        return servers;
    }

    public void setServers(String servers){
        this.servers = Arrays.asList(servers.split(";"));
    }

    private Properties properties = new Properties();

    public Properties getProperties(){
        return properties;
    }

    private Ssl ssl = new Ssl();

    public Ssl getSsl(){
        return ssl;
    }

    private Consumer consumer = new Consumer();

    public Consumer getConsumer() {
        return consumer;
    }

    private Topics topics = new Topics();

    public Topics getTopics() {
        return topics;
    }

    private Streams streams = new Streams();

    public Streams getStreams() {
        return streams;
    }

    public static class Consumer{
        public String getAutoOffsetResetConfig() {
            return autoOffsetResetConfig;
        }

        public void setAutoOffsetResetConfig(String autoOffsetResetConfig) {
            this.autoOffsetResetConfig = autoOffsetResetConfig;
        }

        private String autoOffsetResetConfig;
    }

    public static class Ssl {
        private String truststoreLocation;

        public String getTruststoreLocation(){
            return truststoreLocation;
        }

        public void setTruststoreLocation(String truststoreLocation){
            this.truststoreLocation = truststoreLocation;
        }

        private String truststorePassword;

        public String getTruststorePassword(){
            return truststorePassword;
        }

        public void setTruststorePassword(String truststorePassword){
            this.truststorePassword = truststorePassword;
        }

        private String keystoreLocation;

        public String getKeystoreLocation(){
            return keystoreLocation;
        }

        public void setKeystoreLocation(String keystoreLocation){
            this.keystoreLocation = keystoreLocation;
        }

        private String keystorePassword;

        public String getKeystorePassword(){
            return keystorePassword;
        }

        public void setKeystorePassword(String keystorePassword){
            this.keystorePassword = keystorePassword;
        }

        private String keyPassword;

        public String getKeyPassword(){
            return keyPassword;
        }

        public void setKeyPassword(String keyPassword){
            this.keyPassword = keyPassword;
        }
    }

    public static class Properties{
        private String securityProtocol;

        public String getSecurityProtocol(){
            return securityProtocol;
        }

        public void setSecurityProtocol(String securityProtocol){
            this.securityProtocol = securityProtocol;
        }
    }

    public static class Topics {
        private String in;
        private String out;

        public String getIn() {
            return in;
        }

        public void setIn(String in) {
            this.in = in;
        }

        public String getOut() {
            return out;
        }

        public void setOut(String out) {
            this.out = out;
        }
    }

    public static class Streams {
        private String applicationIdConfig;
        private String stateDirConfig;

        public String getApplicationIdConfig() {
            return applicationIdConfig;
        }

        public void setApplicationIdConfig(String applicationIdConfig) {
            this.applicationIdConfig = applicationIdConfig;
        }

        public String getStateDirConfig() {
            return stateDirConfig;
        }

        public void setStateDirConfig(String stateDirConfig) {
            this.stateDirConfig = stateDirConfig;
        }

        private Aggregation aggregation = new Aggregation();

        public Aggregation getAggregation(){
            return aggregation;
        }

        public static class Aggregation {
            private int windowSizeInMinutes;
            private int windowRetentionPeriodInMinutes;

            public int getWindowSizeInMinutes() {
                return windowSizeInMinutes;
            }

            public void setWindowSizeInMinutes(int windowSizeInMinutes) {
                this.windowSizeInMinutes = windowSizeInMinutes;
            }

            public int getWindowRetentionPeriodInMinutes() {
                return windowRetentionPeriodInMinutes;
            }

            public void setWindowRetentionPeriodInMinutes(int windowRetentionPeriodInMinutes) {
                this.windowRetentionPeriodInMinutes = windowRetentionPeriodInMinutes;
            }
        }
    }
}

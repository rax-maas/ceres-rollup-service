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
        private String outLevel1;
        private String outLevel2;
        private String outLevel3;
        private String outLevel4;
        private String outLevel5;

        public String getOutLevel1() {
            return outLevel1;
        }

        public void setOutLevel1(String outLevel1) {
            this.outLevel1 = outLevel1;
        }

        public String getOutLevel2() {
            return outLevel2;
        }

        public void setOutLevel2(String outLevel2) {
            this.outLevel2 = outLevel2;
        }

        public String getOutLevel3() {
            return outLevel3;
        }

        public void setOutLevel3(String outLevel3) {
            this.outLevel3 = outLevel3;
        }

        public String getOutLevel4() {
            return outLevel4;
        }

        public void setOutLevel4(String outLevel4) {
            this.outLevel4 = outLevel4;
        }

        public String getOutLevel5() {
            return outLevel5;
        }

        public void setOutLevel5(String outLevel5) {
            this.outLevel5 = outLevel5;
        }

        public String getIn() {
            return in;
        }

        public void setIn(String in) {
            this.in = in;
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
            private int lateArrivalInMinutes;
            private int windowSizeInMinutesLevel1;
            private int windowSizeInMinutesLevel2;
            private int windowSizeInMinutesLevel3;
            private int windowSizeInMinutesLevel4;
            private int windowSizeInMinutesLevel5;

            public int getWindowSizeInMinutesLevel1() {
                return windowSizeInMinutesLevel1;
            }

            public void setWindowSizeInMinutesLevel1(int windowSizeInMinutesLevel1) {
                this.windowSizeInMinutesLevel1 = windowSizeInMinutesLevel1;
            }

            public int getWindowSizeInMinutesLevel2() {
                return windowSizeInMinutesLevel2;
            }

            public void setWindowSizeInMinutesLevel2(int windowSizeInMinutesLevel2) {
                this.windowSizeInMinutesLevel2 = windowSizeInMinutesLevel2;
            }

            public int getWindowSizeInMinutesLevel3() {
                return windowSizeInMinutesLevel3;
            }

            public void setWindowSizeInMinutesLevel3(int windowSizeInMinutesLevel3) {
                this.windowSizeInMinutesLevel3 = windowSizeInMinutesLevel3;
            }

            public int getWindowSizeInMinutesLevel4() {
                return windowSizeInMinutesLevel4;
            }

            public void setWindowSizeInMinutesLevel4(int windowSizeInMinutesLevel4) {
                this.windowSizeInMinutesLevel4 = windowSizeInMinutesLevel4;
            }

            public int getWindowSizeInMinutesLevel5() {
                return windowSizeInMinutesLevel5;
            }

            public void setWindowSizeInMinutesLevel5(int windowSizeInMinutesLevel5) {
                this.windowSizeInMinutesLevel5 = windowSizeInMinutesLevel5;
            }

            public int getLateArrivalInMinutes() {
                return lateArrivalInMinutes;
            }

            public void setLateArrivalInMinutes(int lateArrivalInMinutes) {
                this.lateArrivalInMinutes = lateArrivalInMinutes;
            }
        }
    }
}

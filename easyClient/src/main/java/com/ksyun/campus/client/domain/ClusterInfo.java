package com.ksyun.campus.client.domain;

import java.util.List;

public class ClusterInfo {
    private MetaServerMsg masterMetaServer;
    private MetaServerMsg slaveMetaServer;
    private List<DataServerMsg> dataServer;

    public MetaServerMsg getMasterMetaServer() {
        return masterMetaServer;
    }

    public void setMasterMetaServer(MetaServerMsg masterMetaServer) {
        this.masterMetaServer = masterMetaServer;
    }

    public MetaServerMsg getSlaveMetaServer() {
        return slaveMetaServer;
    }

    public void setSlaveMetaServer(MetaServerMsg slaveMetaServer) {
        this.slaveMetaServer = slaveMetaServer;
    }

    public List<DataServerMsg> getDataServer() {
        return dataServer;
    }

    public void setDataServer(List<DataServerMsg> dataServer) {
        this.dataServer = dataServer;
    }

    public class MetaServerMsg{
        private String host;
        private int port;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }
    public class DataServerMsg{
        private String host;
        private int port;
        private int fileTotal;
        private int capacity;
        private int useCapacity;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getFileTotal() {
            return fileTotal;
        }

        public void setFileTotal(int fileTotal) {
            this.fileTotal = fileTotal;
        }

        public int getCapacity() {
            return capacity;
        }

        public void setCapacity(int capacity) {
            this.capacity = capacity;
        }

        public int getUseCapacity() {
            return useCapacity;
        }

        public void setUseCapacity(int useCapacity) {
            this.useCapacity = useCapacity;
        }
    }

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "masterMetaServer=" + masterMetaServer +
                ", slaveMetaServer=" + slaveMetaServer +
                ", dataServer=" + dataServer +
                '}';
    }
}

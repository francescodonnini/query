package io.github.francescodonnini;

public class Conf {
    private final String sparkMaster;
    private final int sparkMasterPort;
    private final String sparkAppName;
    private final String hdfsHost;
    private final int hdfsPort;
    private final String filePath;

    public Conf(String sparkMaster,int sparkMasterPort, String sparkAppName, String hdfsHost, int hdfsPort, String filePath) {
        this.sparkMaster = sparkMaster;
        this.sparkMasterPort = sparkMasterPort;
        this.sparkAppName = sparkAppName;
        this.hdfsHost = hdfsHost;
        this.hdfsPort = hdfsPort;
        this.filePath = filePath;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public int getSparkMasterPort() {
        return sparkMasterPort;
    }

    public String getSparkAppName() {
        return sparkAppName;
    }

    public String getHdfsHost() {
        return hdfsHost;
    }

    public int getHdfsPort() {
        return hdfsPort;
    }

    public String getFilePath() {
        return filePath;
    }
}

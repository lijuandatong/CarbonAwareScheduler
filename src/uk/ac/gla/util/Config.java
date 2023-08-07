package uk.ac.gla.util;

public class Config {
    private String appName;
    private String sparkMaster;
    private String workloadId;
    private String dataSetPath;
    private String dataSize;
    private int iterations;
    private int interruptions;
    private int curStep;
    private String executionLogPath;
    private String dbTb;
    private String logPath;
    private String modelPath;
    private String bucket;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getDbTb() {
        return dbTb;
    }

    public void setDbTb(String dbTb) {
        this.dbTb = dbTb;
    }

    public String getDataSize() {
        return dataSize;
    }

    public void setDataSize(String dataSize) {
        this.dataSize = dataSize;
    }

    public int getInterruptions() {
        return interruptions;
    }

    public void setInterruptions(int interruptions) {
        this.interruptions = interruptions;
    }

    public int getCurStep() {
        return curStep;
    }

    public void setCurStep(int curStep) {
        this.curStep = curStep;
    }

    public String getModelPath() {
        return modelPath;
    }

    public void setModelPath(String modelPath) {
        this.modelPath = modelPath;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public void setSparkMaster(String sparkMaster) {
        this.sparkMaster = sparkMaster;
    }

    public String getWorkloadId() {
        return workloadId;
    }

    public void setWorkloadId(String workloadId) {
        this.workloadId = workloadId;
    }

    public String getDataSetPath() {
        return dataSetPath;
    }

    public void setDataSetPath(String dataSetPath) {
        this.dataSetPath = dataSetPath;
    }

    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }

    public String getExecutionLogPath() {
        return executionLogPath;
    }

    public void setExecutionLogPath(String executionLogPath) {
        this.executionLogPath = executionLogPath;
    }

    public String getLogPath() {
        return logPath;
    }

    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }
}

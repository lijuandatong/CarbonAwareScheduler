package uk.ac.gla.util;

import org.apache.spark.scheduler.*;

import java.sql.*;

public class CustomSparkListener extends SparkListener {
    private static long startTime;
    private static long endTime;
    private Connection connection;
    private Config config;

    public CustomSparkListener(Config config){
        this.config = config;
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        startTime = applicationStart.time();
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        endTime = applicationEnd.time();
        System.out.println("The runtime of this application is：" + (int)((endTime - startTime) / 1000));
        String[] record = new String[11];
        record[0] = config.getAppName();
        record[1] = config.getWorkloadId();
        record[2] = config.getSparkMaster();
        record[3] = config.getDataSize();
        record[4] = String.valueOf(config.getIterations());
        record[5] = String.valueOf(Util.NUM_CLUSTERS);
        record[6] = String.valueOf(config.getInterruptions());
        record[7] = String.valueOf(config.getCurStep());
        record[8] = Util.getTime(startTime);
        record[9] = Util.getTime(endTime);
        record[10] = String.valueOf((int)((endTime - startTime) / 1000));

        FileUtil.writeRecordToCsvFile(config.getExecutionLogPath(), record);
    }
}

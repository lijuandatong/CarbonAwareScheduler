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
    public void onJobStart(SparkListenerJobStart jobStart) {
        System.out.println("job开始，job id为：" + jobStart.jobId());
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        System.out.println("应用开始，时间为：" + applicationStart.time());
        startTime = applicationStart.time();
//        try {
//            Class.forName("com.mysql.cj.jdbc.Driver");
//            connection = DriverManager.getConnection(config.getDbPath());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        System.out.println("应用结束，时间为：" + applicationEnd.time());
        System.out.println("应用开始时间为：" + startTime);
        endTime = applicationEnd.time();
        System.out.println("该应用所用时长：" + (int)((endTime - startTime) / 1000));
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

//        // write to database
//        PreparedStatement statement = null;
//        try {
//            String sql = "INSERT INTO " + config.getDbTb()
//                    + " (app_name,workload_id,spark_master,data_set_size,iterations,clusters,interruption,cur_step,start_time,end_time,duration_sec) "
//                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
//            statement = connection.prepareStatement(sql);
//            statement.setString(1, config.getAppName());
//            statement.setString(2, config.getWorkloadId());
//            statement.setString(3, config.getSparkMaster());
//            statement.setString(4, config.getDataSize());
//            statement.setInt(5, config.getIterations());
//            statement.setInt(6, Util.NUM_CLUSTERS);
//            statement.setInt(7, config.getInterruptions());
//            statement.setInt(8, config.getCurStep());
//            statement.setDate(9, new Date(startTime));
//            statement.setDate(10, new Date(endTime));
//            statement.setInt(11, (int)((endTime - startTime) / 1000));
//            int rows = statement.executeUpdate();
//            System.out.println("数据影响" + rows + "条");
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            if(statement != null){
//                try {
//                    statement.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            if(connection != null){
//                try {
//                    connection.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//
//        }
    }
}

package uk.ac.gla.util;

import org.apache.spark.scheduler.*;

import java.sql.*;

public class CustomSparkListener extends SparkListener {
    private long startTime;
    private long endTime;
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
        Statement statement = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(config.getDbPath());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(statement != null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        System.out.println("应用结束，时间为：" + applicationEnd.time());
        endTime = applicationEnd.time();
        // write to database
        PreparedStatement statement = null;
        try {
            String sql = "INSERT INTO " + config.getDbTb()
                    + " (app_name,workload_id,spark_master,data_set_size,iterations,clusters,interruption,cur_step,start_time,end_time,duration_sec) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
            statement = connection.prepareStatement(sql);
            statement.setString(1, config.getAppName());
            statement.setString(2, config.getWorkloadId());
            statement.setString(3, config.getSparkMaster());
            statement.setString(4, config.getDataSize());
            statement.setInt(5, config.getIterations() + 1);
            statement.setInt(6, Util.NUM_CLUSTERS);
            statement.setInt(7, config.getInterruptions());
            statement.setInt(8, config.getCurStep());
            statement.setDate(9, new Date(startTime));
            statement.setDate(10, new Date(endTime));
            statement.setInt(11, (int)((endTime - startTime) / 1000));
            int rows = statement.executeUpdate();
            System.out.println("数据影响" + rows + "条");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(statement != null){
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }

//    @Override
//    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
//        System.out.println("stage完成，具体信息：" + stageCompleted.stageInfo().details());
//    }
//
//    @Override
//    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
//        System.out.println("stage提交，具体信息：" + stageSubmitted.stageInfo().details());
//    }
//
//    @Override
//    public void onTaskStart(SparkListenerTaskStart taskStart) {
//        System.out.println("任务开始，任务id为：" + taskStart.taskInfo().taskId());
//    }
//
//    @Override
//    public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
//        System.out.println("任务获取结果，时长为：" + taskGettingResult.taskInfo().duration());
//    }
//
//    @Override
//    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
//        System.out.println("任务结束，任务id为：" + taskEnd.taskInfo().taskId());
//    }
//
//    @Override
//    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
//        super.onEnvironmentUpdate(environmentUpdate);
//    }
//
//    @Override
//    public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
//        super.onBlockManagerAdded(blockManagerAdded);
//    }
//
//    @Override
//    public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
//        super.onBlockManagerRemoved(blockManagerRemoved);
//    }
//
//    @Override
//    public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
//        super.onUnpersistRDD(unpersistRDD);
//    }
//
//
//    @Override
//    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
//        super.onExecutorMetricsUpdate(executorMetricsUpdate);
//    }
//
//    @Override
//    public void onStageExecutorMetrics(SparkListenerStageExecutorMetrics executorMetrics) {
//        super.onStageExecutorMetrics(executorMetrics);
//    }
//
//    @Override
//    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
//        super.onExecutorAdded(executorAdded);
//    }
//
//    @Override
//    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
//        super.onExecutorRemoved(executorRemoved);
//    }
//
//    @Override
//    public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
//        super.onExecutorBlacklisted(executorBlacklisted);
//    }
//
//    @Override
//    public void onExecutorExcluded(SparkListenerExecutorExcluded executorExcluded) {
//        super.onExecutorExcluded(executorExcluded);
//    }
//
//    @Override
//    public void onExecutorBlacklistedForStage(SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage) {
//        super.onExecutorBlacklistedForStage(executorBlacklistedForStage);
//    }
//
//    @Override
//    public void onExecutorExcludedForStage(SparkListenerExecutorExcludedForStage executorExcludedForStage) {
//        super.onExecutorExcludedForStage(executorExcludedForStage);
//    }
//
//    @Override
//    public void onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
//        super.onNodeBlacklistedForStage(nodeBlacklistedForStage);
//    }
//
//    @Override
//    public void onNodeExcludedForStage(SparkListenerNodeExcludedForStage nodeExcludedForStage) {
//        super.onNodeExcludedForStage(nodeExcludedForStage);
//    }
//
//    @Override
//    public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted) {
//        super.onExecutorUnblacklisted(executorUnblacklisted);
//    }
//
//    @Override
//    public void onExecutorUnexcluded(SparkListenerExecutorUnexcluded executorUnexcluded) {
//        super.onExecutorUnexcluded(executorUnexcluded);
//    }
//
//    @Override
//    public void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {
//        super.onNodeBlacklisted(nodeBlacklisted);
//    }
//
//    @Override
//    public void onNodeExcluded(SparkListenerNodeExcluded nodeExcluded) {
//        super.onNodeExcluded(nodeExcluded);
//    }
//
//    @Override
//    public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {
//        super.onNodeUnblacklisted(nodeUnblacklisted);
//    }
//
//    @Override
//    public void onNodeUnexcluded(SparkListenerNodeUnexcluded nodeUnexcluded) {
//        super.onNodeUnexcluded(nodeUnexcluded);
//    }
//
//    @Override
//    public void onUnschedulableTaskSetAdded(SparkListenerUnschedulableTaskSetAdded unschedulableTaskSetAdded) {
//        super.onUnschedulableTaskSetAdded(unschedulableTaskSetAdded);
//    }
//
//    @Override
//    public void onUnschedulableTaskSetRemoved(SparkListenerUnschedulableTaskSetRemoved unschedulableTaskSetRemoved) {
//        super.onUnschedulableTaskSetRemoved(unschedulableTaskSetRemoved);
//    }
//
//    @Override
//    public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
//        super.onBlockUpdated(blockUpdated);
//    }
//
//    @Override
//    public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask) {
//        super.onSpeculativeTaskSubmitted(speculativeTask);
//    }
//
//    @Override
//    public void onOtherEvent(SparkListenerEvent event) {
//        super.onOtherEvent(event);
//    }

//    @Override
//    public void onResourceProfileAdded(SparkListenerResourceProfileAdded event) {
//        super.onResourceProfileAdded(event);
//    }
}

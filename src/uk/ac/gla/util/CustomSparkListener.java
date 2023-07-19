package uk.ac.gla.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.scheduler.*;

import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

public class CustomSparkListener extends SparkListener {
    private int jobCount = 0;
    private JavaSparkContext sparkContext;
    private Timer timer;

    public CustomSparkListener(JavaSparkContext sparkContext){
        this.sparkContext = sparkContext;
    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        System.out.println("job开始，job id为：" + jobStart.jobId());
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        System.out.println("job结束，job id为：" + jobEnd.jobId());
        jobCount++;
        if(jobCount % 5 == 0){
            System.out.println("执行完5个job了，你可以在这做一些操作");
            // 取消spark作业
//            sparkContext.cancelAllJobs();

            // ERROR AsyncEventQueue: Listener CustomSparkListener threw an exception
            //org.apache.spark.SparkException: Cannot stop SparkContext within listener bus thread.
//            sparkContext.close();
//            if(timer == null){
//                timer = new Timer();
//            }
//            timer.schedule(new TimerTask() {
//                @Override
//                public void run() {
//                    System.out.println("30秒到，重新提交作业");
//                    System.out.println(sparkContext);
//                    System.out.println("--------------任务重启分割线------------");
//
//                    JavaRDD<String> data = sparkContext.textFile("data/kmeans_input_data.txt");
//                    JavaRDD<Vector> parsedData = data.map(s -> {
//                        double[] values = Arrays.stream(s.split(" "))
//                                .mapToDouble(Double::parseDouble)
//                                .toArray();
//                        return Vectors.dense(values);
//                    });
//                    parsedData.cache();
//
//                    KMeansModel clusters = KMeans.train(parsedData.rdd(), Util.NUM_CLUSTERS, Util.MAX_ITERATIONS);
//                    Vector[] vectors = clusters.clusterCenters();
//                    System.out.println("kmeans迭代算出的簇中心分别为：");
//                    for (int i = 0; i < vectors.length; i++) {
//                        System.out.println(Arrays.toString(vectors[i].toArray()));
//                    }
//
//                }
//            }, 30000);

        }
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        System.out.println("应用开始，时间为：" + applicationStart.time());
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        System.out.println("应用结束，时间为：" + applicationEnd.time());
        if(timer != null){
            timer.cancel();
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

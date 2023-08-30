package uk.ac.gla.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import uk.ac.gla.util.Config;
import uk.ac.gla.util.CustomSparkListener;
import uk.ac.gla.util.FileUtil;
import uk.ac.gla.util.Util;

import java.text.DecimalFormat;
import java.util.*;


public class CustomPageRankJob {
    private static Config config;
    private static Timer timer;
    private static int steps = Util.NUM_STEPS;
    private static int interationsPerStep;
    private static int curStep = 0;
    private static int numIteration = 0;

    public static void main(String[] args) {
        String sparkMasterDef;
        String workloadId;
        String root;
        int iterations;
        int interruptions;
        if(args == null || args.length == 0){
            workloadId = "local_workload_02";
            root = "";
            iterations = Util.NUM_ITERATION;
            interruptions = Util.NUM_STEPS - 1;
            sparkMasterDef = "local[4]"; // default is local mode with two executors
//            executionLogPath = "jdbc:mysql://localhost:3306/master_project_database?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Europe/London&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&user=root&password=root";
        }else{
            workloadId = args[0];
            sparkMasterDef = args[1];
            root = args[2]; // the name of bucket: dataproc-staging-europe-north1-50159985750-plelxggi
            iterations = Integer.valueOf(args[3]);
            interruptions = Integer.valueOf(args[4]);
        }
        config = new Config();
        config.setAppName("PageRank");
        config.setSparkMaster(sparkMasterDef);
        config.setWorkloadId(workloadId);
        config.setBucket(root);
        if(config.getSparkMaster().equals("yarn")){
            root = "gs://" + config.getBucket() + "/";
        }
        String dataSetPath = root + Util.PAGERANK_DATA_SET_RELATIVE_PATH;
        config.setDataSetPath(dataSetPath);
        String modelPath = root + Util.PAGERANK_MODEL_RELATIVE_PATH;
        config.setModelPath(modelPath);

        config.setExecutionLogPath(root + "results/execution_log.csv");
        config.setDbTb("t_workload_history_step_by_step");
        config.setIterations(iterations);
        config.setLogPath(root + "data/log/spark-events-by-step");
        // The number of interruptions
        config.setInterruptions(interruptions);

        steps = interruptions + 1;
        interationsPerStep = Util.getNumIterationPerStep(config);
        System.out.println("The number of iterations:" + iterations);
        System.out.println("The number of steps:" + steps);
        System.out.println("The number of iterations per step:" + interationsPerStep);

        // get the size of file
        long fileSize = FileUtil.getFileSize(dataSetPath);
        if(fileSize != 0){
            config.setDataSize(new DecimalFormat("#.0").format(fileSize / 1024.0 / 1024.0 / 1024.0) + "GB");
        }

        // clear the model generated last time
        FileUtil.deleteFile(config.getModelPath());
        System.out.println("Delete the model generated in last time");

        JavaSparkContext sparkContext = initSparkContext(config);
        submitSparkJob(sparkContext);
    }

    private static void submitSparkJob(JavaSparkContext javaSparkContext) {
        JavaPairRDD<String, Iterable<String>> links = getLinks(javaSparkContext);
        JavaPairRDD<String, Double> ranks = getRanks(javaSparkContext, links);

        curStep++;
        config.setCurStep(curStep);
        System.out.println("The " + curStep + " step starts");

        // run Util.NUM_ITERATION_PER_STEP iterations in one step
        int curIterations = interationsPerStep;
        if(curStep == steps){
            curIterations = config.getIterations() - interationsPerStep * (steps - 1);
        }
        System.out.println("The current step is: " + curStep);
        System.out.println("The number of iterations in this step is:" + curIterations);

        for (int i = 1; i <= curIterations; i++) {
            JavaPairRDD<String, Double> contributions = links.join(ranks).values()
                    .flatMapToPair(pair -> {
                        Iterable<String> urls = pair._1();
                        double rank = pair._2();
                        int size = 0;
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String url : urls) {
                            size++;
                        }
                        for (String url : urls) {
                            results.add(new Tuple2<>(url, rank / size));
                        }
                        return results.iterator();
                    });
            ranks = contributions.reduceByKey(Double::sum)
                    .mapToPair(pair -> new Tuple2<>(pair._1(), 0.15 + 0.85 * pair._2()));
            List<Tuple2<String, Double>> records = ranks.take(10);
            for(Tuple2<String, Double> data : records){
                System.out.println("calculate model is Key: " + data._1() + ", Value: " + data._2());
            }
        }

        numIteration = numIteration + curIterations;
        System.out.println("The " + numIteration + " iteration end");

        saveRanks(ranks);

        if(curStep == steps){
            System.out.println("all jobs complete");
            if(timer != null){
                timer.cancel();
            }
            javaSparkContext.close();
            return;
        }
        // stop the spark after one step complete
        setTimer();
        javaSparkContext.close(); // Close the spark session
        System.out.println("The " + curStep + " step have completed");
        System.out.println("The next step will start after 30s");
    }

    private static JavaPairRDD<String, Iterable<String>> getLinks(JavaSparkContext javaSparkContext){
        // load the data set
        JavaPairRDD<String, Iterable<String>> links = javaSparkContext.textFile(config.getDataSetPath())
                .mapToPair(line -> {
                    String[] parts = line.split("\t");
                    return new Tuple2<>(parts[0], parts[1]);
                })
                .distinct()
                .groupByKey()
                .cache();
//        links.foreach(data -> System.out.println(data));
        return links;
    }

    private static JavaPairRDD<String, Double> getRanks(JavaSparkContext javaSparkContext, JavaPairRDD<String, Iterable<String>> links) {
        JavaPairRDD<String, Double> ranks = getRanksFromFile(javaSparkContext);
        if(ranks == null){
            // init rank 1.0
            System.out.println("The pagerank model is null, init ranks 1.0");
            ranks = links.mapValues(v -> 1.0);
        }
        return ranks;
    }

    private static void saveRanks(JavaPairRDD<String, Double> ranks){
        FileUtil.deleteFile(config.getModelPath());
        ranks.saveAsTextFile(config.getModelPath());
        System.out.println("rank model is saved");
    }

    private static JavaPairRDD<String, Double> getRanksFromFile(JavaSparkContext sparkContext) {
        boolean isModelExist = FileUtil.isHadoopDirectoryExist(config.getModelPath());
        if(isModelExist){
            System.out.println("Get the model from the file successfully.");
            System.out.println("model path 为：" + config.getModelPath());
            JavaPairRDD<String, Double> ranks = sparkContext.textFile(config.getModelPath()).mapToPair(line -> {
                String[] parts = line.substring(1, line.length() - 1).split(",");
                String key = parts[0];
                Double value = Double.parseDouble(parts[1]);
                return new Tuple2<>(key, value);
            });
            return ranks;
        }
        return null;
    }

    private static JavaSparkContext initSparkContext(Config config) {
        // Create the Spark Configuration
        SparkConf conf = new SparkConf()
                .setAppName(config.getAppName())
                .setMaster(config.getSparkMaster())
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", config.getLogPath());

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        javaSparkContext.sc().addSparkListener(new CustomSparkListener(config));
        return javaSparkContext;
    }

    private static void setTimer() {
        if(timer == null){
            timer = new Timer();
        }
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("Time is up, resubmit the job.");
                submitSparkJob(initSparkContext(config));
            }
        }, 120000);
    }
}

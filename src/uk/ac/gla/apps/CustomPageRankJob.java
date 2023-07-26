package uk.ac.gla.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;
import uk.ac.gla.util.Config;
import uk.ac.gla.util.CustomSparkListener;
import uk.ac.gla.util.Util;

import java.io.File;
import java.text.DecimalFormat;
import java.util.*;


public class CustomPageRankJob {
    private static Config config;
    private static Timer timer;
    private static int steps = Util.NUM_STEPS;
    private static int interationsPerStep = Util.NUM_ITERATION_PER_STEP;
    private static int curStep = 0;
    private static int numIteration = 0;

    public static void main(String[] args) {
        String sparkMasterDef;
        String workloadId;
        String dataSetPathRoot;
        int iterations;
        int interruptions;
        String dbPath;
        String logPathRoot;
        if(args == null || args.length == 0){
            // local
            File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
            System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

            workloadId = "local_workload_02";
            dataSetPathRoot = "data/";
            iterations = Util.NUM_ITERATION;
            interruptions = Util.NUM_STEPS - 1;
            sparkMasterDef = "local[4]"; // default is local mode with two executors
            logPathRoot = "E:\\glasgow\\CS\\bigData\\teamProject\\MasterProject\\data\\log\\";
            dbPath = "jdbc:mysql://localhost:3306/master_project_database?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Europe/London&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&user=root&password=root";
        }else{
            workloadId = args[0];
            sparkMasterDef = args[1];
            dataSetPathRoot = args[2];
            iterations = Integer.valueOf(args[3]);
            interruptions = Integer.valueOf(args[4]);
            logPathRoot = args[5];
            dbPath = args[6];
        }
        steps = interruptions + 1;
        interationsPerStep = iterations % steps == 0 ? (iterations / steps) : (iterations / steps + 1);
        System.out.println("初始总迭代次数为：" + iterations);
        System.out.println("总共步骤为：：" + steps);
        System.out.println("每步的迭代次数：" + interationsPerStep);
        String dataSetPath = dataSetPathRoot + Util.PAGERANK_DATA_SET_PATH;

        String modelPath = dataSetPathRoot + "pagerank_model";
        File file = new File(modelPath);
        if(file.exists()){
            deleteFolder(file);
        }

        config = new Config();

        File inputData = new File(dataSetPath);
        if(inputData.exists()){
            double length =  inputData.length() / 1024.0 / 1024.0 / 1024.0;
            config.setDataSize(new DecimalFormat("#.0").format(length) + "GB");
        }

        config.setAppName("PageRank");
        config.setSparkMaster(sparkMasterDef);
        config.setDataSetPath(dataSetPath);
        config.setDbPath(dbPath);
        config.setDbTb("t_workload_history_step_by_step");
        config.setWorkloadId(workloadId);
        config.setIterations(iterations);
        config.setLogPath(logPathRoot + "spark-events-by-step");
        config.setModelPath(modelPath);
        // 打断次数 打断2次，则分三个步骤执行
        config.setInterruptions(interruptions);

        JavaSparkContext sparkContext = initSparkContext(config);
        submitSparkJob(sparkContext);
    }

    private static void submitSparkJob(JavaSparkContext javaSparkContext) {
        JavaPairRDD<String, Double> ranks = getRanks(javaSparkContext);

        curStep++;
        config.setCurStep(curStep);
        System.out.println("The " + curStep + " step starts");

        // run Util.NUM_ITERATION_PER_STEP iterations in one step
        int curIterations = interationsPerStep;
        if(curStep == steps){
            curIterations = config.getIterations() - interationsPerStep * (steps - 1);
        }
        System.out.println("目前是第 " + curStep + "步");
        System.out.println("本步迭代要迭代 " + curIterations + " 次");

        for (int i = 1; i <= curIterations; i++) {
            JavaPairRDD<String, Double> contributions = getLinks(javaSparkContext).join(ranks).values()
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

//            ranks.foreach(data -> System.out.println(data));
        }
        ranks.foreach(data -> System.out.println(data));

        numIteration = numIteration + curIterations;
        System.out.println("The " + numIteration + " iteration end");

        if(curStep == steps){
            System.out.println("all jobs complete");
            if(timer != null){
                timer.cancel();
            }
            javaSparkContext.close();
            return;
        }
        saveRanks(ranks);
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
                    String[] parts = line.split(" ");
                    return new Tuple2<>(parts[0], parts[1]);
                })
                .distinct()
                .groupByKey()
                .cache();
//        links.foreach(data -> System.out.println(data));
        return links;
    }

    private static JavaPairRDD<String, Double> getRanks(JavaSparkContext javaSparkContext) {
        JavaPairRDD<String, Double> ranks = getRanksFromFile(javaSparkContext);
        if(ranks == null){
            // init rank 1.0
            System.out.println("init ranks 1.0");
            ranks = getLinks(javaSparkContext).mapValues(v -> 1.0);
//        ranks.foreach(data -> System.out.println(data));
        }
        return ranks;
    }

    private static void saveRanks(JavaPairRDD<String, Double> ranks){
        File file = new File(config.getModelPath());
        if(file.exists()){
            deleteFolder(file);
        }
        ranks.saveAsTextFile(config.getModelPath());
    }

    private static JavaPairRDD<String, Double> getRanksFromFile(JavaSparkContext sparkContext) {
        File file = new File(config.getModelPath());
        if(file.exists()){
            System.out.println("Get the model from the file successfully.");
            JavaPairRDD<String, Double> ranks = sparkContext.textFile(config.getModelPath()).mapToPair(line -> {
                String[] parts = line.substring(1, line.length() - 1).split(",");
                System.out.println("从文件中读出来：" + parts[0]);
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
//        javaSparkContext.sc().addSparkListener(new CustomSparkListener(config));
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
        }, 5000);
    }

    private static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) {
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}

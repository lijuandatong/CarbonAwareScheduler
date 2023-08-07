package uk.ac.gla.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import uk.ac.gla.util.Config;
import uk.ac.gla.util.CustomSparkListener;
import uk.ac.gla.util.Util;

import java.io.File;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

public class KMeansJob {

    public static void main(String[] args) {
        TimeZone tz = Calendar.getInstance().getTimeZone();
        System.out.println("系统时区为：" + tz.getID());

        String sparkMasterDef;
        String workloadId;
        String dataSetPathRoot;
        int iterations;
        String dbPath;
        String logPathRoot;
        if(args == null || args.length == 0){
            // local
            File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
            System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

            workloadId = "local_workload_01";
            dataSetPathRoot = "data/";
            iterations = Util.NUM_ITERATION;
            sparkMasterDef = "local[4]"; // default is local mode with two executors
            logPathRoot = "E:\\glasgow\\CS\\bigData\\teamProject\\MasterProject\\data\\log\\";
            dbPath = "jdbc:mysql://localhost:3306/master_project_database?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=Europe/London&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&user=root&password=root";
        }else{
            workloadId = args[0];
            sparkMasterDef = args[1];
            dataSetPathRoot = args[2];
            iterations = Integer.valueOf(args[3]);
            dbPath = args[4];
            logPathRoot = args[5];
        }

        String dataSetPath = dataSetPathRoot + Util.KMEANS_DATA_SET_PATH;
        // Generator the input data set
//        String[] kmeansArgs = new String[3];
//        kmeansArgs[0] = String.valueOf(Util.NUM_DATASETS);
//        kmeansArgs[1] = String.valueOf(Util.NUM_CLUSTERS);
//        kmeansArgs[2] = "data/kmeans_input_data3.txt";
//        KMeansDataGenerator.main(kmeansArgs);

        Config config = new Config();

        File file = new File(dataSetPath);
        if(file.exists()){
            double length =  file.length() / 1024.0 / 1024.0 / 1024.0;
            config.setDataSize(new DecimalFormat("#.00").format(length) + "GB");
        }

        config.setAppName("K-Means");
        config.setSparkMaster(sparkMasterDef);
        config.setDataSetPath(dataSetPath);
        config.setExecutionLogPath(dbPath);
        config.setDbTb("t_workload_history_at_once");
        config.setWorkloadId(workloadId);
        config.setIterations(iterations);
        config.setInterruptions(0);
        config.setLogPath(logPathRoot + "spark-events");
        config.setCurStep(1);

        // Create the Spark Configuration
        SparkConf conf = new SparkConf()
                .setAppName(config.getAppName())
                .setMaster(config.getSparkMaster())
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", config.getLogPath());
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        // set spark listener to listen the process
        javaSparkContext.sc().addSparkListener(new CustomSparkListener(config));

//        startTime = System.currentTimeMillis();
//        System.out.println("开始时间：" + startTime);

        JavaRDD<String> data = javaSparkContext.textFile(dataSetPath);
        JavaRDD<Vector> parsedData = data.map(s -> {
            String[] strArray = s.split(" ");
            double[] values = new double[strArray.length];
            for (int i = 0; i < strArray.length; i++) {
                values[i] = Double.parseDouble(strArray[i]);
            }
            return Vectors.dense(values);
        });
        parsedData.cache();

        KMeansModel clusters = new KMeans()
                .setK(Util.NUM_CLUSTERS)
                .setInitializationMode("random")
                .setMaxIterations(Util.NUM_ITERATION + 1)
                .setEpsilon(0)
                .setSeed(1L)
                .run(parsedData.rdd());

        Vector[] vectors = clusters.clusterCenters();
        System.out.println("The cluster centers are：");
        for (int i = 0; i < vectors.length; i++) {
            System.out.println(Arrays.toString(vectors[i].toArray()));
        }
    }
}

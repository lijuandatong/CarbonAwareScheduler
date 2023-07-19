package uk.ac.gla.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import uk.ac.gla.util.CustomSparkListener;
import uk.ac.gla.util.KMeansDataGenerator;
import uk.ac.gla.util.Util;

import java.io.File;
import java.util.Arrays;

public class KmeansSparkJob {
    private static long startTime;
    private static long endTime;

    public static void main(String[] args) {
        // Generator the input data and output to file
        String outPutPathRoot;
        String sparkMasterDef = null;

        if(args == null || args.length == 0){
            // local
            File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
            System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

            outPutPathRoot = "data/";
            sparkMasterDef = "local[4]"; // default is local mode with two executors
        }else{
            outPutPathRoot = args[0];
            sparkMasterDef = args[1];
            System.out.println("spark master:" + sparkMasterDef);
        }

//        String outPutPath = outPutPathRoot + "kmeans_input_data.txt";

//        String[] kmeansArgs = new String[3];
//        kmeansArgs[0] = String.valueOf(Util.NUM_DATASETS);
//        kmeansArgs[1] = String.valueOf(Util.NUM_CLUSTERS);
//        kmeansArgs[2] = "data/kmeans_input_data.txt";
//        KMeansDataGenerator.main(kmeansArgs);

        if (sparkMasterDef==null) sparkMasterDef = "local[4]"; // default is local mode with two executors

        String sparkSessionName = "Kmeans"; // give the session a name

        // Create the Spark Configuration
        SparkConf conf = new SparkConf()
                .setAppName(sparkSessionName)
                .setMaster(sparkMasterDef)
                .set("spark.eventLog.enabled", "true")
                .set("spark.eventLog.dir", "E:\\glasgow\\CS\\bigData\\teamProject\\MasterProject\\data\\log\\spark-events");

        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        // set spark listener to listen the process
        javaSparkContext.sc().addSparkListener(new CustomSparkListener(javaSparkContext));

        startTime = System.currentTimeMillis();
        System.out.println("开始时间：" + startTime);

        JavaRDD<String> data = javaSparkContext.textFile("data/kmeans_input_data1.txt");
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
                .setK(Util.NUM_CLUSTERS1)
                .setInitializationMode("random")
                .setMaxIterations(Util.NUM_ITERATION + 1)
                .setEpsilon(0)
                .setSeed(1L)
                .run(parsedData.rdd());

        Vector[] vectors = clusters.clusterCenters();
        System.out.println("kmeans迭代算出的簇中心分别为：");
        for (int i = 0; i < vectors.length; i++) {
            System.out.println(Arrays.toString(vectors[i].toArray()));
        }

//        try {
//            Thread.sleep(3600 * 1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }


        endTime = System.currentTimeMillis();
        System.out.println("结束时间：" + endTime);
        long cost = endTime - startTime;
        System.out.println("总耗时 " + ((cost / 1000) / 60.0) + " minutes");
    }
}

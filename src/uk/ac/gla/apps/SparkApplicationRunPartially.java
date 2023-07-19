package uk.ac.gla.apps;

import java.io.*;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import uk.ac.gla.util.Util;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class SparkApplicationRunPartially {
	private static Timer timer;
	private static String inputDataPath;
	private static String modelPath = "data/kmeans_model";
	private static String sparkSessionName = "Kmeans"; // give the session a name
	private static String sparkMasterDef = "local[4]"; // default is local mode with two executors
	private static String outPutPathRoot = "data/"; // default is local file directory
	private static int curStep = 0;
	private static int numIteration = 0;
	private static long startTime;
	private static long endTime;

	public static void main(String[] args) {
		if(args == null || args.length == 0){
			// local
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		}else{
			outPutPathRoot = args[0];
			sparkMasterDef = args[1];
			System.out.println("spark master:" + sparkMasterDef);
		}

		// Generator the input data and output to file
//		String outPutPath = outPutPathRoot + "kmeans_input_data.txt";

//		String[] kmeansArgs = new String[3];
//        kmeansArgs[0] = String.valueOf(20);
//        kmeansArgs[1] = String.valueOf(Util.NUM_CLUSTERS);
//        kmeansArgs[2] = "data/kmeans_input_data.txt";
//        KMeansDataGenerator.main(kmeansArgs);

		inputDataPath = outPutPathRoot + "kmeans_input_data1.txt";

		File file = new File(modelPath);
		if(file.exists()){
			file.delete();
		}

		// create SparkContext
		JavaSparkContext javaSparkContext = initSparkContext();
		startTime = System.currentTimeMillis();
		System.out.println("开始时间：" + startTime);
		// submit a spark job
		submitSparkJob(javaSparkContext);
	}

	private static JavaSparkContext initSparkContext() {
		// Create the Spark Configuration
		SparkConf conf = new SparkConf()
				.setAppName(sparkSessionName)
				.setMaster(sparkMasterDef)
				.set("spark.eventLog.enabled", "true")
				.set("spark.eventLog.dir", "E:\\glasgow\\CS\\bigData\\teamProject\\MasterProject\\data\\log\\spark-events-by-step");

		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		return javaSparkContext;
	}

	private static void submitSparkJob(JavaSparkContext javaSparkContext) {
		KMeansModel kMeansModel = getKMeansModel(javaSparkContext);
		Vector[] vectors = kMeansModel.clusterCenters();
		System.out.println("initial cluster centers are：");
		for (int i = 0; i < vectors.length; i++) {
			System.out.println(Arrays.toString(vectors[i].toArray()));
		}

		curStep++;
		System.out.println("The " + curStep + " step starts");

		for(int j = 0; j < Util.NUM_ITERATION_PER_STEP; j++){
			kMeansModel = new KMeans()
					.setK(Util.NUM_CLUSTERS1)
					.setMaxIterations(1)
					.setInitialModel(kMeansModel)
					.run(getInputDataSetRDD(javaSparkContext));

			System.out.println("The " + (++numIteration) + " iteration");
			Vector[] vectors1 = kMeansModel.clusterCenters();
			System.out.println("cluster centers are：");
			for (int i = 0; i < vectors1.length; i++) {
				System.out.println(Arrays.toString(vectors1[i].toArray()));
			}
		}

		saveKMeansModel(javaSparkContext.sc(), kMeansModel);
		// stop the spark after one step complete
		if(curStep == Util.NUM_STEPS){
			System.out.println("all jobs complete");
			if(timer != null){
				timer.cancel();
			}
			javaSparkContext.close();
			endTime = System.currentTimeMillis();
			System.out.println("结束时间：" + endTime);
			long cost = endTime - startTime;
			System.out.println("总耗时 " + ((cost / 1000) / 60.0) + " minutes");
			return;
		}
		setTimer();
		javaSparkContext.close(); // Close the spark session
		System.out.println("The " + curStep + " step have completed");
		System.out.println("The next step will start after 30s");
	}

	private static void setTimer() {
		if(timer == null){
			timer = new Timer();
		}
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				System.out.println("Time is up, resubmit the job.");
				submitSparkJob(initSparkContext());
			}
		}, 5000);
	}

	/**
	 * Get KmeansModel from the intermediate result file first,
	 * if the kmeansModel is null, let kemans iterate one time and get the kmeansModel.
	 * @param javaSparkContext
	 * @return
	 */
	private static KMeansModel getKMeansModel(JavaSparkContext javaSparkContext) {
		KMeansModel kMeansModel = getKMeansModelFromFile(javaSparkContext.sc());
		if(kMeansModel == null){
			System.out.println("Not get model from the file");
			kMeansModel = new KMeans()
					.setK(Util.NUM_CLUSTERS1)
					.setInitializationMode("random")
					.setSeed(1L)
					.setMaxIterations(1)
					.run(getInputDataSetRDD(javaSparkContext));
		}
		return kMeansModel;
	}

	private static RDD getInputDataSetRDD(JavaSparkContext javaSparkContext) {
		JavaRDD<String> data = javaSparkContext.textFile(inputDataPath);
		JavaRDD<Vector> parsedData = data.map(s -> {
			double[] values = Arrays.stream(s.split(" "))
					.mapToDouble(Double::parseDouble)
					.toArray();
			return Vectors.dense(values);
		});
		parsedData.cache();
		return parsedData.rdd();
	}


	private static KMeansModel getKMeansModelFromFile(SparkContext context) {
//        File file = new File(modelPath);
//        if(file.exists()){
//			System.out.println("Get the model from the file successfully.");
//            return KMeansModel.load(context, modelPath);
//        }
//        return null;

		FileInputStream fileInputStream = null;
		ObjectInputStream objectInputStream = null;
		KMeansModel model = null;
		try {
			fileInputStream = new FileInputStream(modelPath);
			objectInputStream = new ObjectInputStream(fileInputStream);
			model = (KMeansModel) objectInputStream.readObject();
			System.out.println("Get the model from the file successfully.");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(objectInputStream != null){
					objectInputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

			try {
				if(fileInputStream != null){
					fileInputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return model;
	}

	private static void saveKMeansModel(SparkContext context, KMeansModel model) {
//		File file = new File(modelPath);
//		if(file.exists()){
//			file.delete();
//		}
//	    model.save(context, modelPath);

		FileOutputStream fileOut = null;
		ObjectOutputStream out = null;
		try {
			fileOut = new FileOutputStream(modelPath);
			// 先清空数据
			fileOut.write(new byte[0]);
			out = new ObjectOutputStream(fileOut);
			out.writeObject(model);
			System.out.println("The step " + curStep + " will end after a while");
			System.out.println("KemeansModel have been saved.");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(out != null){
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(fileOut != null){
					fileOut.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

}


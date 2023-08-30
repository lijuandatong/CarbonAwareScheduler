package uk.ac.gla.apps;

import java.io.*;
import java.text.DecimalFormat;
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
import uk.ac.gla.util.Config;
import uk.ac.gla.util.CustomSparkListener;
import uk.ac.gla.util.FileUtil;
import uk.ac.gla.util.Util;

public class KMeansJobStepByStep {
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
			// local
			workloadId = "local_workload_01";
			root = "";
			iterations = Util.NUM_ITERATION;
			interruptions = Util.NUM_STEPS - 1;
			sparkMasterDef = "local[4]"; // default is local mode with two executors
		}else{
			workloadId = args[0];
			sparkMasterDef = args[1];
			root = args[2]; // The name of bucket: dataproc-staging-europe-north1-50159985750-plelxggi
			iterations = Integer.valueOf(args[3]);
			interruptions = Integer.valueOf(args[4]);
		}

		config = new Config();
		config.setAppName("K-Means");
		config.setSparkMaster(sparkMasterDef);
		config.setWorkloadId(workloadId);
		config.setBucket(root);
		if(config.getSparkMaster().equals("yarn")){
			root = "gs://" + config.getBucket() + "/";
		}
		String dataSetPath = root + Util.KMEANS_DATA_SET_PATH;
		config.setDataSetPath(dataSetPath);
		String modelPath = root + Util.KMEANS_MODEL_PATH;
		config.setModelPath(modelPath);
		config.setInitialModelPath(root + "data/K-Means_initial_model");

		config.setExecutionLogPath(root + "results/execution_log.csv");
		config.setIterations(iterations);
		config.setLogPath(root + "data/log/spark-events-by-step");
		config.setInterruptions(interruptions);

		steps = interruptions + 1;
		interationsPerStep = Util.getNumIterationPerStep(config);
		System.out.println("The number of iterations is :" + iterations);
		System.out.println("The number of steps is :" + steps);
		System.out.println("The number of iterations of each step is :" + interationsPerStep);

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

	private static void submitSparkJob(JavaSparkContext javaSparkContext) {
		KMeansModel kMeansModel = getKMeansModel(javaSparkContext);
		Vector[] vectors = kMeansModel.clusterCenters();
		System.out.println("initial cluster centers are：");
		for (int i = 0; i < vectors.length; i++) {
			System.out.println(Arrays.toString(vectors[i].toArray()));
		}

		curStep++;
		config.setCurStep(curStep);
		System.out.println("The " + curStep + " step starts");

		// run Util.NUM_ITERATION_PER_STEP iterations in one step
		int curIterations = interationsPerStep;
		if(curStep == steps){
			curIterations = config.getIterations() - interationsPerStep * (steps - 1);
		}
		System.out.println("The current step is: " + curStep);
		System.out.println("The number of iterations in this step is :" + curIterations);

		kMeansModel = new KMeans()
				.setK(Util.NUM_CLUSTERS)
				.setEpsilon(0)
				.setMaxIterations(curIterations)
				.setInitialModel(kMeansModel)
				.run(getInputDataSetRDD(javaSparkContext));

		numIteration = numIteration + curIterations;
		System.out.println("The " + numIteration + " iteration end");
		Vector[] vectors1 = kMeansModel.clusterCenters();
		System.out.println("cluster centers are：");
		for (int i = 0; i < vectors1.length; i++) {
			System.out.println(Arrays.toString(vectors1[i].toArray()));
		}

		saveKMeansModel(javaSparkContext.sc(), kMeansModel);

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

	/**
	 * Get KmeansModel from the intermediate result file first,
	 * if the kmeansModel is null, let kemans iterate one time and get the kmeansModel.
	 * @param javaSparkContext
	 * @return
	 */
	private static KMeansModel getKMeansModel(JavaSparkContext javaSparkContext) {
		KMeansModel kMeansModel = getKMeansModelFromFile(javaSparkContext.sc());
		if(kMeansModel == null){
			System.out.println("Not get model from the file, init model");
			kMeansModel = new KMeans()
					.setK(Util.NUM_CLUSTERS)
					.setInitializationMode("random")
					.setSeed(1L)
					.setMaxIterations(1)
					.run(getInputDataSetRDD(javaSparkContext));
		}
		return kMeansModel;
	}

	private static RDD getInputDataSetRDD(JavaSparkContext javaSparkContext) {
		JavaRDD<String> data = javaSparkContext.textFile(config.getDataSetPath());
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
		boolean isModelExist = FileUtil.isHadoopDirectoryExist(config.getModelPath());
		if(isModelExist){
			System.out.println("Get the model from the file successfully.");
			return KMeansModel.load(context, config.getModelPath());
		}
		return null;
	}

	private static void saveKMeansModel(SparkContext context, KMeansModel model) {
		FileUtil.deleteFile(config.getModelPath());
	    model.save(context, config.getModelPath());
	}

	private static void generateInitialKMeansModel(JavaSparkContext context) {
		KMeansModel kMeansModel = new KMeans()
				.setK(Util.NUM_CLUSTERS)
				.setInitializationMode("random")
				.setSeed(1L)
				.setMaxIterations(1)
				.run(getInputDataSetRDD(context));
		kMeansModel.save(context.sc(), config.getInitialModelPath());
	}
}


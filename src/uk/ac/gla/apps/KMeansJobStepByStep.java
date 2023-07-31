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
public class KMeansJobStepByStep {
	private static Config config;
	private static Timer timer;
	private static int steps = Util.NUM_STEPS;
	private static int interationsPerStep = Util.getNumIterationPerStep();
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

			workloadId = "local_workload_01";
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
		interationsPerStep = Util.getNumIterationPerStep();
		System.out.println("初始总迭代次数为：" + iterations);
		System.out.println("总共步骤为：：" + steps);
		System.out.println("每步的迭代次数：" + interationsPerStep);
		String dataSetPath = dataSetPathRoot + Util.KMEANS_DATA_SET_PATH;

		// Generator the input data and output to file
//		String[] kmeansArgs = new String[3];
//        kmeansArgs[0] = String.valueOf(20);
//        kmeansArgs[1] = String.valueOf(Util.NUM_CLUSTERS);
//        kmeansArgs[2] = "data/kmeans_input_data.txt";
//        KMeansDataGenerator.main(kmeansArgs);

		String modelPath = dataSetPathRoot + "K-Means_model";
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

		config.setAppName("K-Means");
		config.setSparkMaster(sparkMasterDef);
		config.setDataSetPath(dataSetPath);
		config.setDbPath(dbPath);
		config.setDbTb("t_workload_history_step_by_step");
		config.setWorkloadId(workloadId);
		config.setIterations(iterations + 1);
		config.setLogPath(logPathRoot + "spark-events-by-step");
		config.setModelPath(modelPath);
		// 打断次数 打断2次，则分三个步骤执行
		config.setInterruptions(interruptions);

		// create SparkContext
		JavaSparkContext javaSparkContext = initSparkContext(config);
		// submit a spark job
		submitSparkJob(javaSparkContext);
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
			curIterations = (config.getIterations() - 1) - interationsPerStep * (steps - 1);
		}
		System.out.println("目前是第 " + curStep + "步");
		System.out.println("本步迭代要迭代 " + curIterations + " 次");

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

		if(curStep == steps){
			System.out.println("all jobs complete");
			if(timer != null){
				timer.cancel();
			}
			javaSparkContext.close();
			return;
		}
		saveKMeansModel(javaSparkContext.sc(), kMeansModel);
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
        File file = new File(config.getModelPath());
        if(file.exists()){
			System.out.println("Get the model from the file successfully.");
            return KMeansModel.load(context, config.getModelPath());
        }
        return null;

//		FileInputStream fileInputStream = null;
//		ObjectInputStream objectInputStream = null;
//		KMeansModel model = null;
//		try {
//			fileInputStream = new FileInputStream(modelPath);
//			objectInputStream = new ObjectInputStream(fileInputStream);
//			model = (KMeansModel) objectInputStream.readObject();
//			System.out.println("Get the model from the file successfully.");
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if(objectInputStream != null){
//					objectInputStream.close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//
//			try {
//				if(fileInputStream != null){
//					fileInputStream.close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//		return model;
	}

	private static void saveKMeansModel(SparkContext context, KMeansModel model) {
		File file = new File(config.getModelPath());
		if(file.exists()){
			deleteFolder(file);
		}
	    model.save(context, config.getModelPath());

//		FileOutputStream fileOut = null;
//		ObjectOutputStream out = null;
//		try {
//			fileOut = new FileOutputStream(modelPath);
//			// 先清空数据
//			fileOut.write(new byte[0]);
//			out = new ObjectOutputStream(fileOut);
//			out.writeObject(model);
//			System.out.println("The step " + curStep + " will end after a while");
//			System.out.println("KemeansModel have been saved.");
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if(out != null){
//					out.close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//			try {
//				if(fileOut != null){
//					fileOut.close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}

	}

	public static void deleteFolder(File folder) {
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


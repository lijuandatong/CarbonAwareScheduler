package uk.ac.gla.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public final class Util {
//    public static final int NUM_CLUSTERS1 = 8;
//    public static final int NUM_DATASETS1 = 00000000; //200,000,000 7.78G 41min// 400,000,000 15G 70min//40,000,000 1.57G   // 50,000,000 1.96G

    public static final int NUM_ITERATION = 10;
    public static final int NUM_STEPS = 2;  // 2, 3, 4, 5, 6, 7, 8 --> interruption: 1, 2, 3, 4, 5, 6, 7

    public static final int NUM_CLUSTERS = 8;
    public static final int NUM_DATASETS = 100000000; //50,000,000
    public static final String KMEANS_DATA_SET_PATH = "data/kmeans_input_data1.txt";
    public static final String KMEANS_MODEL_PATH = "data/K-Means_model";

    public static final String PAGERANK_DATA_SET_RELATIVE_PATH = "data/web-Google.txt";
    public static final String PAGERANK_MODEL_RELATIVE_PATH = "data/pagerank_model";

    public static int getNumIterationPerStep(Config config){
        int steps = config.getInterruptions() + 1;
        int iterations = config.getIterations();
        if(steps > (iterations / 2)){
            return iterations / steps;
        }else{
            return iterations % steps == 0 ? (iterations / steps) : (iterations / steps + 1);
        }
    }

    public static String getTime(long timestamp){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(timestamp);
        return sdf.format(date);
    }
}

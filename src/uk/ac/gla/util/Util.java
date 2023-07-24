package uk.ac.gla.util;

public final class Util {
//    public static final int NUM_CLUSTERS1 = 8;
//    public static final int NUM_DATASETS1 = 00000000; //200,000,000 7.78G 41min// 400,000,000 15G 70min//40,000,000 1.57G   // 50,000,000 1.96G

    public static final int NUM_ITERATION = 10;
    public static final int NUM_STEPS = 3;  // 2, 3, 4, 5, 6, 7, 8 --> interruption: 1, 2, 3, 4, 5, 6, 7

    public static final int NUM_ITERATION_PER_STEP = NUM_ITERATION % NUM_STEPS == 0 ? (NUM_ITERATION / NUM_STEPS) : (NUM_ITERATION / NUM_STEPS + 1);


    public static final int NUM_CLUSTERS = 8;
    public static final int NUM_DATASETS = 100000000; //50,000,000
}

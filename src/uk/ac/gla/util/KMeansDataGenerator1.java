package uk.ac.gla.util;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KMeansDataGenerator1 {
    private static final int DIM = 10;
    private static final double STD_DEV = 0.012;
    private static final Random RAND = new Random(0);

    /**
     * generate kmeans input data set
     * @param n the number of data set
     * @param k the number of cluster
     * @param outputRoot the path of output file
     */
    public static String generateData(int n, int k, String outputRoot){
//        if (args.length < 3) {
//            System.out.println("Usage: KMeansDataGenerator <samples> <clusters> <output>");
//            System.exit(1);
//        }
//
//        int n = Integer.parseInt(args[0]);
//        int k = Integer.parseInt(args[1]);
//        String outputPath = args[2];

        System.out.println("输入数据放入的目录为：" + outputRoot);
        List<RealVector> centers = uniformRandomCenters(DIM, k);
        System.out.println("产生的" + k + "个簇中心为：");
        for(RealVector vector : centers){
            System.out.println(Arrays.toString(vector.toArray()));
        }

        if(outputRoot.startsWith("gs:")){
            // google cloud enviroment
            // Instantiate a Cloud Storage client
            Storage storage = StorageOptions.getDefaultInstance().getService();
            // Prepare the blob ID
            BlobId blobId = BlobId.of(outputRoot, "input/kmeans_input_data.txt");
            // Prepare the blob information
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
            // Create the file in GCS
            storage.create(blobInfo, new byte[0]);
            System.out.println("File created in GCS: " + blobId.getName());

            try (WriteChannel writer = storage.writer(blobInfo)) {
                StringBuilder sb = new StringBuilder();
                IntStream.range(0, n).forEach(i -> {
                    RealVector center = centers.get(RAND.nextInt(k));
                    RealVector point = center.add(gaussianVector(DIM, STD_DEV));
                    for(int j = 0; j < DIM; j++){
                        sb.append(point.toArray()[j]);
                        if(j != DIM - 1){
                            sb.append(" ");
                        }else{
                            if(i != n - 1){
                                sb.append("\n");
                            }
                        }
                    }
                });
                // Convert the string to bytes and wrap it in a ByteBuffer
                ByteBuffer buffer = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));

                // Write the buffer to GCS
                writer.write(buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return outputRoot + "input/kmeans_input_data.txt";
        }else{
            try (PrintWriter writer = new PrintWriter(Paths.get(outputRoot + "kmeans_input_data.txt").toFile())) {
                IntStream.range(0, n).forEach(i -> {
                    RealVector center = centers.get(RAND.nextInt(k));
                    RealVector point = center.add(gaussianVector(DIM, STD_DEV));
                    for(int j = 0; j < DIM; j++){
                        if(j == DIM - 1){
                            if(i == (n - 1)){
                                writer.print(point.toArray()[j]);
                            }else{
                                writer.println(point.toArray()[j]);
                            }
                        }else{
                            writer.print(point.toArray()[j]);
                            writer.print(" ");
                        }
                    }
                });
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            return outputRoot + "kmeans_input_data.txt";
        }
    }

    private static List<RealVector> uniformRandomCenters(int dim, int k) {
        return IntStream.range(0, k).mapToObj(i ->
                new ArrayRealVector(IntStream.range(0, dim).mapToDouble(d -> RAND.nextDouble()).toArray())
        ).collect(Collectors.toList());
    }

    private static RealVector gaussianVector(int dim, double stdDev) {
        NormalDistribution dist = new NormalDistribution(0, stdDev);
        return new ArrayRealVector(IntStream.range(0, dim).mapToDouble(i -> dist.sample()).toArray());
    }
}

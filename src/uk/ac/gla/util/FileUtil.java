package uk.ac.gla.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;

public class FileUtil {

    public static void deleteFile(String directoryName){
        System.out.println("The file is " + directoryName);
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(directoryName), conf);

            if (fs.exists(new Path(directoryName))) {
                fs.delete(new Path(directoryName), true); // "true" represents directory
                System.out.println(directoryName + " is deleted");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean isHadoopDirectoryExist(String directoryName){
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(directoryName), conf);

            if (fs.exists(new Path(directoryName))) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    public static long getFileSize(String filePath){
        long fileSize = 0;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(filePath), conf);

            if (fs.exists(new Path(filePath))) {
                fileSize = fs.getFileStatus(new Path(filePath)).getLen();
                System.out.println("File size: " + fileSize + " bytes.");
            } else {
                System.out.println("File does not exist.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fileSize;
    }

    /**
     * Write a record to csv file
     * @param filePath
     * @param record
     */
    public static void writeRecordToCsvFile(String filePath, String[] record){
        FSDataOutputStream outputStream = null;
        PrintWriter writer = null;
        Path path = new Path(filePath);
        try {
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(new URI(filePath), configuration);
            if(fs.exists(path)){
                outputStream = fs.append(path);
            }else{
                outputStream = fs.create(path);
            }
            writer = new PrintWriter(outputStream);

            // Write the data
            writer.print("\n");
            writer.print(String.join(",", record));
            System.out.println("Write a record successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(writer != null){
                writer.close();
            }
            try {
                if(outputStream != null){
                    outputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

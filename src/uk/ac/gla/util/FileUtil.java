package uk.ac.gla.util;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class FileUtil {
    /**
     *
     * @param path gs://dataproc-staging-europe-north1-50159985750-plelxggi/data/pagerank_model
     *             存储桶的名字为：dataproc-staging-europe-north1-50159985750-plelxggi
     */
    public static void deleteFileFromGCS(String bucket, String path) {
        // 创建Google Cloud Storage客户端
        Storage storage = StorageOptions.getDefaultInstance().getService();
        // 列出所有以指定前缀开始的对象
        Iterable<Blob> blobs = storage.list(bucket, Storage.BlobListOption.prefix(path)).iterateAll();
        if(blobs == null){
            return;
        }

        // 删除每个对象
        for (Blob blob : blobs) {
            boolean deleted = blob.delete();
            if (deleted) {
                System.out.println("Object deleted: " + blob.getName());
            } else {
                System.out.println("Failed to delete object: " + blob.getName());
            }
        }
    }

    public static void deleteFolderFromLocal(File folder) {
        // local
        File[] files = folder.listFiles();
        if(files!=null) {
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolderFromLocal(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

    public static void deleteFile(String directoryName){
        System.out.println("The file is " + directoryName);
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(directoryName), conf);

            if (fs.exists(new Path(directoryName))) {
                fs.delete(new Path(directoryName), true); // "true" 代表如果是目录则递归删除
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

package uk.ac.gla.scheduler;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        LocalDateTime now = LocalDateTime.now();
        // YYYY-MM-DDThh:mmZ
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'");
        String formattedTime = now.format(formatter);

        HttpResponse response = null;
        try {
            HttpClient httpClient = HttpClients.createDefault();
            String url = "https://api.carbonintensity.org.uk/intensity/" + formattedTime + "/fw24h";
            // 创建 HttpGet 请求
            HttpGet httpGet = new HttpGet(url);
            // 发起请求并获取响应
            response = httpClient.execute(httpGet);
            String responseBody = EntityUtils.toString(response.getEntity());
            System.out.println(responseBody);

//            StringBuilder builder = null;
//            BufferedReader br = null;
//            FileReader fr = null;
//            try {
//                fr = new FileReader("E:\\glasgow\\CS\\bigData\\teamProject\\MasterProject\\src\\uk\\ac\\gla\\scheduler\\carbonforcast.txt");
//                br = new BufferedReader(fr);
//                builder = new StringBuilder();
//                String line;
//                while((line = br.readLine()) != null){
//                    builder.append(line);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            } finally {
//                if(br != null){
//                    br.close();
//                }
//                if(fr != null){
//                    fr.close();
//                }
//            }
//            responseBody = builder.toString();

            JSONArray jsonArray = (JSONArray) JSONObject.parse(responseBody).get("data");
            List<CarbonIntensityWindow> windows = new ArrayList<>();
            for (Object obj : jsonArray) {
                CarbonIntensityWindow window = JSON.parseObject(obj.toString(), CarbonIntensityWindow.class);
                windows.add(window);
            }
            System.out.println("Carbon intensity predicted for the next 24 hours:");
            System.out.println(windows.toString());
            System.out.println("The size of windows is : " + windows.size());

            Job job = new Job();
            job.setRuntime(2.3);
            job.setOverheadsPercentage(0.05);
            job.setIterations(50);
            System.out.println("This is a " + job.getRuntime() + " hours job with " + job.getIterations() + " iterations");

            Scheduler scheduler = new Scheduler(job, windows);
            System.out.println("-------------------------------------------------------");
            Result result2 = scheduler.scheduleWithLessInterruptions1();
            System.out.println("-------------------------------------------------------");
            Result result1 = scheduler.scheduleImmediately();
            System.out.println("-------------------------------------------------------");
            Result result3 = scheduler.scheduleWithoutInterruptions();

            Evaluator evaluator = new Evaluator();
            System.out.println("--------------------Evaluate start-----------------------------------");
            System.out.println("Compare to running immediately.");
            double saved1 = evaluator.compare(result1, result2);
            System.out.println("-------------------------------------------------------");
            System.out.println("Compare to running in the consecutive windows without interruptions.");
            double saved2 = evaluator.compare(result3, result2);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

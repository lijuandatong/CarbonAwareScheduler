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
            job.setRuntime(2);
            job.setOverheadsPerInterruption(0.05);
            job.setIterations(40);

            Scheduler scheduler = new Scheduler(job, windows);
            System.out.println("-------------------------------------------------------");
            Result result2 = scheduler.scheduleWithLessInterruptions();
            System.out.println("-------------------------------------------------------");
            Result result1 = scheduler.scheduleImmediately();
            System.out.println("-------------------------------------------------------");
            Result result3 = scheduler.scheduleWithNoInterruptions();
            System.out.println("-------------------------------------------------------");

            Evaluator evaluator = new Evaluator();
            double saved1 = evaluator.compare(result1.getBestWindows(), result2.getBestWindows());
            double saved2 = evaluator.compare(result3.getBestWindows(), result2.getBestWindows());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

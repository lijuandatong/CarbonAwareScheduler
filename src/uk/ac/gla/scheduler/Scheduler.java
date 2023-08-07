package uk.ac.gla.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Scheduler {
    private List<CarbonIntensityWindow> windows;
    private Job job;
    private double unit = 0.5; // 0.5h

    public Scheduler(Job job, List<CarbonIntensityWindow> windows) {
        this.job = job;
        this.windows = windows;
    }

    /**
     * 打断任务，打断次数少的方案
     * job 2h 5% 40
     * @return
     */
    public Result scheduleWithLessInterruptions() {
        System.out.println("The schedule for best windows starts.");
        // TODO: 2023/8/6 迭代次数不能整除的处理
        int needWindowsSize = (int) (job.getRuntime() / unit);  // 4
        System.out.println("The current job need " + needWindowsSize + " windows.");
        double overheads = job.getRuntime() * job.getOverheadsPerInterruption();  // 0.1h
        System.out.println("The current job have " + overheads + " overheads per interruption.");

        List<ExecuteWindow> windows = findBestWindows(needWindowsSize, overheads);
        Result result = new Result();
        result.setBestWindows(windows);
        result.setInterruptions(windows.size() - 1);
        System.out.println(result.toString());
        System.out.println("The schedule for best windows ends.");
        return result;
    }

    private List<ExecuteWindow> findBestWindows(int needWindowsSize, double overheadsPerInterruption) {
        // TODO: 2023/8/7
        // Create a new list for not changing sorting of original list.
        List<CarbonIntensityWindow> sortedWindows = new ArrayList<>(windows);
        // Sort by carbon intensity
        Collections.sort(sortedWindows, new Comparator<CarbonIntensityWindow>() {
            @Override
            public int compare(CarbonIntensityWindow o1, CarbonIntensityWindow o2) {
                return o1.getIntensity().getForecast() - o2.getIntensity().getForecast();
            }
        });
        System.out.println("The windows after sorting : " + sortedWindows.toString());
        // Get the lowest windows
        List<CarbonIntensityWindow> bestWindows = sortedWindows.subList(0, needWindowsSize);
        System.out.println("The top " + needWindowsSize + " windows are : " + bestWindows.toString());
        // Sort by time
        Collections.sort(bestWindows, new Comparator<CarbonIntensityWindow>() {
            @Override
            public int compare(CarbonIntensityWindow o1, CarbonIntensityWindow o2) {
                return o1.getFrom().compareTo(o2.getFrom());
            }
        });
        System.out.println("The best windows after sorting by time: " + bestWindows);

        // 连着的合在一个窗口
        List<ExecuteWindow> executeWindows = new ArrayList<>();
        for(int i = 0; i < needWindowsSize; i++){
            if(i > 0){
                ExecuteWindow executeWindow = executeWindows.get(executeWindows.size() - 1);
                double previousCarbonEmissions = executeWindows.get(executeWindows.size() - 1).getCarbonEmissions();
                if(bestWindows.get(i).getFrom().compareTo(bestWindows.get(i-1).getTo()) == 0){
                    executeWindow.setTo(bestWindows.get(i).getTo());
                    executeWindow.setCarbonEmissions(previousCarbonEmissions
                            + bestWindows.get(i).getIntensity().getForecast() * 0.5);
                    continue;
                }else{
                    // add the overheads
                    executeWindow.setCarbonEmissions(previousCarbonEmissions
                            + getNextWindowIntensity(bestWindows.get(i-1)) * overheadsPerInterruption);
                }
            }
            ExecuteWindow window = new ExecuteWindow();
            window.setFrom(bestWindows.get(i).getFrom());
            window.setTo(bestWindows.get(i).getTo());
            if(i == (needWindowsSize - 1) && executeWindows.size() > 1){
                window.setCarbonEmissions(bestWindows.get(i).getIntensity().getForecast() * 0.5
                        + getNextWindowIntensity(bestWindows.get(i)) * overheadsPerInterruption);
            }else{
                // carbon emissions in half an hour
                window.setCarbonEmissions(bestWindows.get(i).getIntensity().getForecast() * 0.5);
            }
            executeWindows.add(window);
        }
        return executeWindows;
    }

    /**
     * 获取当前窗口相邻窗口的碳强度
     * @param carbonIntensityWindow
     * @return
     */
    private int getNextWindowIntensity(CarbonIntensityWindow carbonIntensityWindow) {
        int index = windows.indexOf(carbonIntensityWindow);
        if(index < (windows.size() - 1)){
            return windows.get(index + 1).getIntensity().getForecast();
        }
        return 0;
    }


    /**
     * 打断任务，打断次数多的方案
     * @return
     */
    public Result scheduleWithMoreInterruptions(){
        return null;
    }

    /**
     * 无碳感知，立即执行
     * @return
     */
    public Result scheduleImmediately(){
        System.out.println("The schedule for running immediately without carbon aware starts.");
        // TODO: 2023/8/6 不能整除的处理
        int needWindowsSize = (int)(job.getRuntime() / unit);  // 4
        System.out.println("The job need " + needWindowsSize + " windows.");
        List<ExecuteWindow> executeWindows = new ArrayList<>();
        ExecuteWindow executeWindow = new ExecuteWindow();
        executeWindow.setFrom(windows.get(0).getFrom());
        executeWindow.setTo(windows.get(needWindowsSize - 1).getTo());
        double totalCarbonEmissions = 0;
        for(int i = 0; i < needWindowsSize; i++){
            CarbonIntensityWindow window = windows.get(i);
            System.out.println("The " + i + " window is: " + window);
            totalCarbonEmissions += window.getIntensity().getForecast() * 0.5;
        }
        executeWindow.setCarbonEmissions(totalCarbonEmissions);
        executeWindows.add(executeWindow);
        Result result = new Result();
        result.setBestWindows(executeWindows);
        result.setInterruptions(executeWindows.size() - 1);
        System.out.println(result.toString());
        System.out.println("The schedule for running immediately without carbon aware ends.");
        return result;
    }

    /**
     * 考虑到碳感知，放在最佳的连续的几个窗口执行
     * @return
     */
    public Result scheduleWithNoInterruptions(){
        System.out.println("The schedule for running in consecutive windows with interruptions starts.");
        // TODO: 2023/8/6 不能整除的处理
        int needWindowsSize = (int)(job.getRuntime() / unit);  // 4
        System.out.println("The job need " + needWindowsSize + " consecutive windows.");
        List<CarbonIntensityWindow> largerWindows = new ArrayList<>();
        for(int i = 0; i < windows.size() - (needWindowsSize - 1); i++){
            CarbonIntensityWindow window = new CarbonIntensityWindow();
            window.setFrom(windows.get(i).getFrom());
            window.setTo(windows.get(i + (needWindowsSize - 1)).getTo());
            int forecast = 0;
            for(int j = i; j < needWindowsSize + i; j++){
                forecast += windows.get(j).getIntensity().getForecast();
            }
            window.setIntensity(new Intensity(forecast));
            largerWindows.add(window);
        }

        Collections.sort(largerWindows, new Comparator<CarbonIntensityWindow>() {
            @Override
            public int compare(CarbonIntensityWindow o1, CarbonIntensityWindow o2) {
                return o1.getIntensity().getForecast() - o2.getIntensity().getForecast();
            }
        });

        System.out.println("The best consecutive windows are: " + largerWindows.get(0));

        List<ExecuteWindow> executeWindows = new ArrayList<>();
        ExecuteWindow executeWindow = new ExecuteWindow();
        executeWindow.setFrom(largerWindows.get(0).getFrom());
        executeWindow.setTo(largerWindows.get(0).getTo());
        executeWindow.setCarbonEmissions(largerWindows.get(0).getIntensity().getForecast() * 0.5);
        executeWindows.add(executeWindow);

        Result result = new Result();
        result.setBestWindows(executeWindows);
        result.setInterruptions(executeWindows.size() - 1);
        System.out.println(result.toString());
        System.out.println("The schedule for running in consecutive windows with interruptions ends.");

        return result;
    }
}

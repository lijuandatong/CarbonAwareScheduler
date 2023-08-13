package uk.ac.gla.scheduler;

import uk.ac.gla.util.Util;

import java.time.LocalDateTime;
import java.util.*;

public class Scheduler {
    private List<CarbonIntensityWindow> windows;
    private Job job;
    private double unit = 0.5; // 0.5h
    private int needWindowsSize = 0;
    private double overheadsPerInterruption = 0;
    private double runtimePerIteration = 0;
//    private int iterationsPerHalfWindow = 0;
    private Map<String, Integer> intensityMap = new HashMap<>();

    public Scheduler(Job job, List<CarbonIntensityWindow> windows) {
        this.job = job;
        this.windows = windows;
        // TODO: 2023/8/6 迭代次数不能整除的处理
        this.needWindowsSize = (int) (Math.ceil(job.getRuntime() / unit));  // 如2h的job，需要4个窗口。如2.3h的job，需要5个窗口向上取整
        System.out.println("The current job need " + needWindowsSize + " windows.");

        overheadsPerInterruption = job.getRuntime() * job.getOverheadsPercentage();  // 0.1h
        System.out.println("The current job have " + overheadsPerInterruption + " h overheads per interruption.");

        runtimePerIteration = job.getRuntime() / job.getIterations();
        System.out.println("The runtime per iteration is " + runtimePerIteration + " h.");

        for (CarbonIntensityWindow carbonIntensityWindow : windows) {
            intensityMap.put(carbonIntensityWindow.getFrom().toString(), carbonIntensityWindow.getIntensity().getForecast());
        }
    }

    /**
     * 打断任务，打断次数少的方案
     * job 2h 5% 40
     *
     * @return
     */
    public Result scheduleWithLessInterruptions() {
        System.out.println("The schedule for best windows starts.");

        List<ExecuteWindow> windows = findBestWindows(needWindowsSize);
        // calculate carbon emissions(add the overheads)
        if (windows.size() > 1) {
            double overheadsPerStep = overheadsPerInterruption * (windows.size() - 1) / windows.size();  // overhead affects all steps roughly evenly
            System.out.println("The overheads per step is: " + overheadsPerStep);
            for (ExecuteWindow window : windows) {
                int intensity = getWindowIntensity(window.getTo().toString());
                System.out.println("相邻窗口的碳强度为：" + intensity);
                window.setCarbonEmissions(window.getCarbonEmissions() + overheadsPerStep * intensity);
            }
        }

        Result result = new Result();
        result.setBestWindows(windows);
        result.setInterruptions(windows.size() - 1);
        System.out.println(result.toString());
        System.out.println("The schedule for best windows ends.");
        return result;
    }

    /**
     * 打断调度
     * 1.找出n个最低碳强度的窗口
     * 2.如果最佳窗口为连续的一个大窗口，则不需要打断
     * 3.如果不连续，则计算每个窗口执行的迭代数，如果是个小数，则向上取整（在该窗口执行不完的，在下一个继续执行）
     * 4.算出每个窗口的碳排放
     * @return
     */
    public Result scheduleWithLessInterruptions1() {
        System.out.println("The schedule for best windows starts.");
        List<ExecuteWindow> windows = findBestWindows(needWindowsSize);
        // calculate carbon emissions(include the overheads)
        double carbonEmissions = 0;
        if (windows.size() > 1) {
            // run with interruptions
            double overheadsPerStep = overheadsPerInterruption * (windows.size() - 1) / windows.size();  // overhead affects all steps roughly evenly
            System.out.println("The overheads per step is: " + overheadsPerStep);
            double itPerHalfWindow = unit / runtimePerIteration; // 每个窗口能够执行的迭代次数，有可能是整数，也可能是小数

            // 计算每次打断前执行几次迭代
            calculateIterationsEveryInterruption(windows, itPerHalfWindow);

            for (int i = 0; i < windows.size(); i++) {
                ExecuteWindow window = windows.get(i);
                int intensityNextHalfWindow = getWindowIntensity(window.getTo().toString());
                System.out.println("相邻窗口的碳强度为：" + intensityNextHalfWindow);

                List<CarbonIntensityWindow> subWindows = window.getSubWindows();
                if(subWindows.size() > 1){
                    // 如果当前窗口是连续的几个窗口
                    for(int j = 0; j < subWindows.size(); j++){
                        CarbonIntensityWindow subWindow = subWindows.get(j);
                        int curIntensity = window.getSubWindows().get(j).getIntensity().getForecast();
                        if(j == subWindows.size() - 1){
                            // 连续窗口的最后一个窗口的处理
                            double restIterations = window.getIterations() - j * itPerHalfWindow;
                            if(restIterations > itPerHalfWindow){
                                // 半小时不够，未执行完的在下一个窗口执行
                                carbonEmissions += getCarbonRunBeyondHalfHourWindow(restIterations,
                                        overheadsPerStep, curIntensity, intensityNextHalfWindow);
                            }else{
                                // 执行小于等于半小时窗口
                                carbonEmissions += getCarbonRunWithinHalfHourWindow(restIterations,
                                        overheadsPerStep,
                                        curIntensity,
                                        intensityNextHalfWindow);
                            }
                        }else{
                            // 连续窗口中的子窗口求碳排放
                            carbonEmissions += unit * subWindow.getIntensity().getForecast();
                        }

                    }
                }else{
                    // 只有一个半小时窗口
                    // 0.5*intensity + （11-0.86）*0.046 * intensity1 + overheads * intensity1
                    int curIntensity = window.getSubWindows().get(0).getIntensity().getForecast();
                    if(window.getIterations() > itPerHalfWindow){
                        // 半小时不够，未执行完的在下一个窗口执行
                        carbonEmissions += getCarbonRunBeyondHalfHourWindow(window.getIterations(),
                                overheadsPerStep, curIntensity, intensityNextHalfWindow);
                    }else{
                        // 执行小于等于半小时窗口
                        carbonEmissions += getCarbonRunWithinHalfHourWindow(window.getIterations(),
                                overheadsPerStep,
                                curIntensity,
                                intensityNextHalfWindow);
                    }
                }
            }
        }else{
            // run in consecutive windows without interruptions
            List<CarbonIntensityWindow> subWindows = windows.get(0).getSubWindows();
            for(int i = 0; i < subWindows.size(); i++){
                CarbonIntensityWindow subWindow = subWindows.get(i);
                if(i == subWindows.size() - 1){
                    carbonEmissions += (job.getRuntime() - unit * (subWindows.size() - 1)) * subWindow.getIntensity().getForecast();
                }else{
                    carbonEmissions += unit * subWindow.getIntensity().getForecast();
                }
            }
        }

        Result result = new Result();
        result.setBestWindows(windows);
        result.setInterruptions(windows.size() - 1);
        result.setCarbonEmissions(Util.formatDecimalWithOneDecimal(carbonEmissions));
        System.out.println(result.toString());
        System.out.println("The schedule for best windows ends.");
        return result;
    }

    /**
     * 计算在打断前所执行的迭代
     */
    private void calculateIterationsEveryInterruption(List<ExecuteWindow> executeWindows, double itPerHalfWindow){
        int restIterations = job.getIterations();
        for(int i = 0; i < executeWindows.size(); i++){
            if(restIterations <= 0){
                return;
            }
            ExecuteWindow window = executeWindows.get(i);
            // 每个窗口能够执行的迭代次数，有可能是整数，也可能是小数
            double itPerWindow = itPerHalfWindow * window.getSubWindows().size();
            int it = Math.min((int) Math.ceil(itPerWindow), restIterations); // 向上取整，执行完此迭代次数打断
            window.setIterations(it);
            restIterations -= it;
        }
    }

    /**
     * 迭代（不包含打断开支）在半个小时窗口执行不完，未完成的在下一个窗口继续执行
     * @param it 迭代数
     * @param overheadsPerStep  打断带来的开支
     * @param curIntensity 当前窗口的碳强度
     * @param nextIntensity 下一个窗口的碳强度
     * @return
     */
    private double getCarbonRunBeyondHalfHourWindow(double it, double overheadsPerStep, int curIntensity, int nextIntensity){
        // 半小时不够，未执行完的在下一个窗口执行
        return curIntensity * unit // 0.5小时执行10.86次的碳排放
                + (it * runtimePerIteration - unit) * nextIntensity // 0.5小时没执行完的放在下一个窗口执行
                + overheadsPerStep * nextIntensity; // 打断带来的开支
    }

    /**
     * 执行时间（此数值不包含打断开支）小于半个窗口的碳排放计算
     * @param iterations 迭代数
     * @param overheadsPerStep 打断带来的开支
     * @param curIntensity 当前窗口的碳强度
     * @param nextIntensity 下个窗口的碳强度
     * @return
     */
    private double getCarbonRunWithinHalfHourWindow(double iterations, double overheadsPerStep, int curIntensity, int nextIntensity){
        // 执行小于等于半小时窗口
        double runtime = iterations * runtimePerIteration;
        double restTime = unit - runtime;
        double carbonOverheads = 0;
        if(overheadsPerStep <= restTime){
            carbonOverheads = overheadsPerStep * curIntensity;
        }else{
            carbonOverheads = restTime * curIntensity
                    + (overheadsPerStep - restTime) * nextIntensity;
        }
        return runtime * curIntensity + carbonOverheads;
    }

    /**
     * 找n个最佳窗口，n为任务不打断需要的窗口数
     * 1.找n个碳强度最小的半小时窗口
     * 2.如果有挨着的窗口合并
     * @param needWindowsSize 任务不打断需要的窗口数
     * @return
     */
    private List<ExecuteWindow> findBestWindows(int needWindowsSize) {
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

        // combine the windows which are neighbours
        List<ExecuteWindow> executeWindows = new ArrayList<>();
        for (int i = 0; i < needWindowsSize; i++) {
            if (i > 0) {
                ExecuteWindow previousWindow = executeWindows.get(executeWindows.size() - 1);
//                double previousCarbonEmissions = executeWindows.get(executeWindows.size() - 1).getCarbonEmissions();
                if (bestWindows.get(i).getFrom().compareTo(previousWindow.getTo()) == 0) {
                    previousWindow.setTo(bestWindows.get(i).getTo());
                    previousWindow.getSubWindows().add(bestWindows.get(i));
                    continue;
                }
            }
            ExecuteWindow window = new ExecuteWindow();
            window.setFrom(bestWindows.get(i).getFrom());
            window.setTo(bestWindows.get(i).getTo());
            List<CarbonIntensityWindow> subWindows = new ArrayList<>();
            subWindows.add(bestWindows.get(i));
            window.setSubWindows(subWindows);
            executeWindows.add(window);
        }
        return executeWindows;
    }

    /**
     * 获取当前窗口相邻窗口的碳强度
     *
     * @param carbonIntensityWindow
     * @return
     */
    private int getNextWindowIntensity(CarbonIntensityWindow carbonIntensityWindow) {
        int index = windows.indexOf(carbonIntensityWindow);
        if (index < (windows.size() - 1)) {
            return windows.get(index + 1).getIntensity().getForecast();
        }
        return 0;
    }

    private int getWindowIntensity(String time) {
        return intensityMap.get(time);
    }


    /**
     * 打断任务，打断次数多的方案
     *
     * @return
     */
    public Result scheduleWithMoreInterruptions() {
        return null;
    }

    /**
     * 无碳感知，立即执行
     *
     * @return
     */
    public Result scheduleImmediately() {
        System.out.println("The schedule for running immediately without carbon aware starts.");
        List<ExecuteWindow> executeWindows = new ArrayList<>();
        ExecuteWindow executeWindow = new ExecuteWindow();
        executeWindow.setFrom(windows.get(0).getFrom());
        executeWindow.setTo(windows.get(needWindowsSize - 1).getTo());
        double totalCarbonEmissions = calculateCarbonEmissions(0, needWindowsSize);
//        for (int i = 0; i < needWindowsSize; i++) {
//            CarbonIntensityWindow window = windows.get(i);
//            System.out.println("The " + i + " window is: " + window);
//            if(i == needWindowsSize - 1){
//                // 最后一个窗口可能执行不满
//                double runTime = job.getRuntime() - (needWindowsSize - 1) * 0.5;
//                totalCarbonEmissions += window.getIntensity().getForecast() * runTime;
//            }else{
//                totalCarbonEmissions += window.getIntensity().getForecast() * 0.5;
//            }
//        }
        executeWindows.add(executeWindow);
        Result result = new Result();
        result.setBestWindows(executeWindows);
        result.setInterruptions(executeWindows.size() - 1);
        result.setCarbonEmissions(Util.formatDecimalWithOneDecimal(totalCarbonEmissions));
        System.out.println(result.toString());
        System.out.println("The schedule for running immediately without carbon aware ends.");
        return result;
    }

    private double calculateCarbonEmissions(int startIndex, int windowSize){
        double carbonEmissions = 0;
        for (int j = startIndex; j < windowSize + startIndex; j++) {
            if(j == (windowSize + startIndex - 1)){
                // 最后一个窗口可能执行不满
                double runTime = job.getRuntime() - (windowSize - 1) * 0.5;
                carbonEmissions += windows.get(j).getIntensity().getForecast() * runTime;
            }else{
                carbonEmissions += windows.get(j).getIntensity().getForecast() * 0.5;
            }
        }
        return carbonEmissions;
    }

    /**
     * 考虑到碳感知，放在最佳的连续的几个窗口执行
     *
     * @return
     */
    public Result scheduleWithoutInterruptions() {
        System.out.println("The schedule for running in consecutive windows without interruptions starts.");
        // 把半小时窗口转换为分钟窗口
        List<CarbonIntensityWindow> minuteWindows = new ArrayList<>();
        for(CarbonIntensityWindow window : windows){
            LocalDateTime startTime = window.getFrom();
            for(int i = 0; i < 30; i++){
                CarbonIntensityWindow minuteWindow = new CarbonIntensityWindow();
                minuteWindow.setFrom(startTime);
//                System.out.println("开始时间：" + startTime);
                LocalDateTime endTime = startTime.plusMinutes(1);
                minuteWindow.setTo(endTime);
//                System.out.println("结束时间：" + endTime);
                minuteWindow.setIntensity(window.getIntensity());
                minuteWindows.add(minuteWindow);

                startTime = endTime;
            }
        }
        List<ExecuteWindow> consecutiveWindows = new ArrayList<>();
        int needWindows = (int) Math.ceil(job.getRuntime() * 60);
        for (int i = 0; i < minuteWindows.size() - (needWindows - 1); i++) {
            ExecuteWindow window = new ExecuteWindow();
            window.setFrom(minuteWindows.get(i).getFrom());
            window.setTo(minuteWindows.get(i + (needWindows - 1)).getTo());
//            double carbonEmissions = calculateCarbonEmissions(i, needWindows);
            double carbonEmissions = 0;
            for (int j = i; j < needWindows + i; j++) {
                carbonEmissions += minuteWindows.get(j).getIntensity().getForecast() * 1;
            }
            window.setCarbonEmissions(carbonEmissions / 60);
            consecutiveWindows.add(window);
        }

        Collections.sort(consecutiveWindows, new Comparator<ExecuteWindow>() {
            @Override
            public int compare(ExecuteWindow o1, ExecuteWindow o2) {
//                System.out.println("");
                return Double.compare(o1.getCarbonEmissions(), o2.getCarbonEmissions());
            }
        });

        System.out.println("The best consecutive windows are: " + consecutiveWindows.get(0));

        List<ExecuteWindow> executeWindows = new ArrayList<>();
        executeWindows.add(consecutiveWindows.get(0));

        Result result = new Result();
        result.setBestWindows(executeWindows);
        result.setInterruptions(executeWindows.size() - 1);
        result.setCarbonEmissions(consecutiveWindows.get(0).getCarbonEmissions());
        System.out.println(result.toString());
        System.out.println("The schedule for running in consecutive windows with interruptions ends.");

        return result;
    }
}

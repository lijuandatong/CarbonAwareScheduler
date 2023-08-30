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
    private Map<String, Integer> intensityMap = new HashMap<>();

    public Scheduler(Job job, List<CarbonIntensityWindow> windows) {
        this.job = job;
        this.windows = windows;
        // if a 2h job, it needs four windows. If a 2.3h job, it needs five windows.
        this.needWindowsSize = (int) (Math.ceil(job.getRuntime() / unit));
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
     * schedule job with interruptions
     * 1.Find n the lowest carbon intensity windows.
     * 2.If these windows are consecutive, this job will not be interrupted.
     * 3.If not consecutive, calculate the number of iterations per window.
     * 4.Calculate carbon emissions.
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
            double itPerHalfWindow = unit / runtimePerIteration;

            // calculate the number of iterations in each window
            calculateIterationsEveryInterruption(windows, itPerHalfWindow);

            for (int i = 0; i < windows.size(); i++) {
                ExecuteWindow window = windows.get(i);
                int intensityNextHalfWindow = getWindowIntensity(window.getTo().toString());
                System.out.println("The intensity of next window is：" + intensityNextHalfWindow);

                List<CarbonIntensityWindow> subWindows = window.getSubWindows();
                if(subWindows.size() > 1){
                    // If a consecutive window
                    for(int j = 0; j < subWindows.size(); j++){
                        CarbonIntensityWindow subWindow = subWindows.get(j);
                        int curIntensity = window.getSubWindows().get(j).getIntensity().getForecast();
                        if(j == subWindows.size() - 1){
                            double restIterations = window.getIterations() - j * itPerHalfWindow;
                            if(restIterations > itPerHalfWindow){
                                carbonEmissions += getCarbonRunBeyondHalfHourWindow(restIterations,
                                        overheadsPerStep, curIntensity, intensityNextHalfWindow);
                            }else{
                                carbonEmissions += getCarbonRunWithinHalfHourWindow(restIterations,
                                        overheadsPerStep,
                                        curIntensity,
                                        intensityNextHalfWindow);
                            }
                        }else{
                            carbonEmissions += unit * subWindow.getIntensity().getForecast();
                        }

                    }
                }else{
                    // 0.5*intensity + （11-0.86）*0.046 * intensity1 + overheads * intensity1
                    int curIntensity = window.getSubWindows().get(0).getIntensity().getForecast();
                    if(window.getIterations() > itPerHalfWindow){
                        carbonEmissions += getCarbonRunBeyondHalfHourWindow(window.getIterations(),
                                overheadsPerStep, curIntensity, intensityNextHalfWindow);
                    }else{
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
     * calculate the iterations per window
     */
    private void calculateIterationsEveryInterruption(List<ExecuteWindow> executeWindows, double itPerHalfWindow){
        int restIterations = job.getIterations();
        for(int i = 0; i < executeWindows.size(); i++){
            if(restIterations <= 0){
                return;
            }
            ExecuteWindow window = executeWindows.get(i);
            double itPerWindow = itPerHalfWindow * window.getSubWindows().size();
            int it = Math.min((int) Math.ceil(itPerWindow), restIterations); // 向上取整，执行完此迭代次数打断
            window.setIterations(it);
            restIterations -= it;
        }
    }

    /**
     * calculate the carbon emissions that runtime is more than an half hour
     * @param it
     * @param overheadsPerStep
     * @param curIntensity
     * @param nextIntensity
     * @return
     */
    private double getCarbonRunBeyondHalfHourWindow(double it, double overheadsPerStep, int curIntensity, int nextIntensity){
        return curIntensity * unit
                + (it * runtimePerIteration - unit) * nextIntensity
                + overheadsPerStep * nextIntensity; // overhead
    }

    /**
     * calculate the carbon emissions that runtime is less than an half hour
     * @param iterations
     * @param overheadsPerStep
     * @param curIntensity intensity of current window
     * @param nextIntensity intensity of next window
     * @return
     */
    private double getCarbonRunWithinHalfHourWindow(double iterations, double overheadsPerStep, int curIntensity, int nextIntensity){
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
     * Find n best windows
     * 1.Find n lowest carbon intensity half hour windows.
     * 2.Combine the consecutive windows.
     * @param needWindowsSize The number of windows needed when job is not interrupted.
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
     * Get the intensity of next window
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
     * Execute immediately
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
                double runTime = job.getRuntime() - (windowSize - 1) * 0.5;
                carbonEmissions += windows.get(j).getIntensity().getForecast() * runTime;
            }else{
                carbonEmissions += windows.get(j).getIntensity().getForecast() * 0.5;
            }
        }
        return carbonEmissions;
    }

    /**
     * Carbon-aware consecutive windows
     *
     * @return
     */
    public Result scheduleWithoutInterruptions() {
        System.out.println("The schedule for running in consecutive windows without interruptions starts.");
        // convert half hour window to minute window
        List<CarbonIntensityWindow> minuteWindows = new ArrayList<>();
        for(CarbonIntensityWindow window : windows){
            LocalDateTime startTime = window.getFrom();
            for(int i = 0; i < 30; i++){
                CarbonIntensityWindow minuteWindow = new CarbonIntensityWindow();
                minuteWindow.setFrom(startTime);
                LocalDateTime endTime = startTime.plusMinutes(1);
                minuteWindow.setTo(endTime);
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

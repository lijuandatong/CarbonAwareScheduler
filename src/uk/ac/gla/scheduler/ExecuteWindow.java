package uk.ac.gla.scheduler;

import java.time.LocalDateTime;
import java.util.List;

public class ExecuteWindow {
    private LocalDateTime from;
    private LocalDateTime to;
    private int iterations; // 一个执行窗口执行多少次迭代
    private List<CarbonIntensityWindow> subWindows; // Execution windows may have several consecutive windows
    private double carbonEmissions; // unit: g (1kw power) = runtime * intensity

    public LocalDateTime getFrom() {
        return from;
    }

    public void setFrom(LocalDateTime from) {
        this.from = from;
    }

    public LocalDateTime getTo() {
        return to;
    }

    public void setTo(LocalDateTime to) {
        this.to = to;
    }

    public double getCarbonEmissions() {
        return carbonEmissions;
    }

    public void setCarbonEmissions(double carbonEmissions) {
        this.carbonEmissions = carbonEmissions;
    }

    public List<CarbonIntensityWindow> getSubWindows() {
        return subWindows;
    }

    public void setSubWindows(List<CarbonIntensityWindow> subWindows) {
        this.subWindows = subWindows;
    }

    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }

    @Override
    public String toString() {
        return "ExecuteWindow{" +
                "from=" + from +
                ", to=" + to +
                ", iterations=" + iterations +
                ", subWindows=" + subWindows +
                ", carbonEmissions=" + carbonEmissions +
                '}';
    }
}

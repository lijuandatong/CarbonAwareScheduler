package uk.ac.gla.scheduler;

import java.util.List;

public class Result {
    private int interruptions;
    // The best execution windows
    private List<ExecuteWindow> bestWindows;
    private double carbonEmissions; // unit: g = runtime * intensity (1kw power)

    public int getInterruptions() {
        return interruptions;
    }

    public void setInterruptions(int interruptions) {
        this.interruptions = interruptions;
    }

    public List<ExecuteWindow> getBestWindows() {
        return bestWindows;
    }

    public void setBestWindows(List<ExecuteWindow> bestWindows) {
        this.bestWindows = bestWindows;
    }

    public double getCarbonEmissions() {
        return carbonEmissions;
    }

    public void setCarbonEmissions(double carbonEmissions) {
        this.carbonEmissions = carbonEmissions;
    }

    @Override
    public String toString() {
        return "Result{" +
                "interruptions=" + interruptions +
                ", bestWindows=" + bestWindows +
                ", carbonEmissions=" + carbonEmissions +
                '}';
    }
}

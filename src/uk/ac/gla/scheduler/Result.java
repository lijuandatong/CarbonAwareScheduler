package uk.ac.gla.scheduler;

import java.util.List;

public class Result {
    private int interruptions;
    // 最佳执行窗口，不包含打断支出
    private List<ExecuteWindow> bestWindows;

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

    @Override
    public String toString() {
        return "Result{" +
                "interruptions=" + interruptions +
                ", bestWindows=" + bestWindows +
                '}';
    }
}

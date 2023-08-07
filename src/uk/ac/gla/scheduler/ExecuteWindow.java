package uk.ac.gla.scheduler;

import java.time.LocalDateTime;

public class ExecuteWindow {
    private LocalDateTime from;
    private LocalDateTime to;
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

    @Override
    public String toString() {
        return "ExecuteWindow{" +
                "from=" + from +
                ", to=" + to +
                ", carbonEmissions=" + carbonEmissions +
                '}';
    }
}

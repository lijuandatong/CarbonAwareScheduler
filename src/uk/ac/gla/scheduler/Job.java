package uk.ac.gla.scheduler;

public class Job {
    private double runtime; // unit is hour
    private double overheadsPercentage;  // overheads per interruption
    private int iterations;

    public double getRuntime() {
        return runtime;
    }

    public void setRuntime(double runtime) {
        this.runtime = runtime;
    }

    public double getOverheadsPercentage() {
        return overheadsPercentage;
    }

    public void setOverheadsPercentage(double overheadsPercentage) {
        this.overheadsPercentage = overheadsPercentage;
    }

    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }
}

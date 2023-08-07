package uk.ac.gla.scheduler;

public class Job {
    private int runtime; // unit is hour
    private double overheadsPerInterruption;
    private int iterations;

    public int getRuntime() {
        return runtime;
    }

    public void setRuntime(int runtime) {
        this.runtime = runtime;
    }

    public double getOverheadsPerInterruption() {
        return overheadsPerInterruption;
    }

    public void setOverheadsPerInterruption(double overheadsPerInterruption) {
        this.overheadsPerInterruption = overheadsPerInterruption;
    }

    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }
}

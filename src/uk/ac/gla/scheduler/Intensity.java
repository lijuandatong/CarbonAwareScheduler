package uk.ac.gla.scheduler;

public class Intensity {
    private int forecast;
    private int actual;
    private String index;

    public Intensity(int forecast) {
        this.forecast = forecast;
    }

    public int getForecast() {
        return forecast;
    }

    public void setForecast(int forecast) {
        this.forecast = forecast;
    }

    public int getActual() {
        return actual;
    }

    public void setActual(int actual) {
        this.actual = actual;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }
}

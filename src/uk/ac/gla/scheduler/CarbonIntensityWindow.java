package uk.ac.gla.scheduler;

import java.time.LocalDateTime;

/***
 * 碳强度预测窗口，大小默认半个小时
 */
public class CarbonIntensityWindow{
    private LocalDateTime from;
    private LocalDateTime to;
    private Intensity intensity;

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

    public Intensity getIntensity() {
        return intensity;
    }

    public void setIntensity(Intensity intensity) {
        this.intensity = intensity;
    }

    @Override
    public String toString() {
        return "CarbonIntensityWindow{" +
                "from=" + from +
                ", to=" + to +
                ", intensity=" + intensity.getForecast();
    }
}

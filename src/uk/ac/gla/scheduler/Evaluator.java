package uk.ac.gla.scheduler;

import java.util.List;

public class Evaluator {
    public double compare(List<ExecuteWindow> otherWindows, List<ExecuteWindow> bestWindows){

        double carbonEmissionsOtherWindows = 0;
        for(ExecuteWindow window : otherWindows){
            carbonEmissionsOtherWindows += window.getCarbonEmissions();
        }
        System.out.println("The carbon emissions are " + carbonEmissionsOtherWindows + " in other strategy.");

        double carbonEmissionsBestWindows = 0;
        for(ExecuteWindow window : bestWindows){
            carbonEmissionsBestWindows += window.getCarbonEmissions();
        }
        System.out.println("The carbon emissions are " + carbonEmissionsBestWindows + " in best windows.");

        double result = (carbonEmissionsOtherWindows - carbonEmissionsBestWindows) / carbonEmissionsOtherWindows;
        System.out.println("The saved carbon emissions are " + result);
        return result;
    }

}

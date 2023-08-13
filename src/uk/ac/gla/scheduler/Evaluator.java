package uk.ac.gla.scheduler;

import uk.ac.gla.util.Util;

public class Evaluator {
    public double compare(Result resultOtherWay, Result resultWithInterrupions){

        System.out.println("The carbon emissions are " + resultOtherWay.getCarbonEmissions());
        System.out.println("The carbon emissions are " + resultWithInterrupions.getCarbonEmissions() + " with interruptions.");

        double result = (resultOtherWay.getCarbonEmissions() - resultWithInterrupions.getCarbonEmissions()) / resultOtherWay.getCarbonEmissions();
        result = Util.formatDecimalWithTwoDecimal(result);
        System.out.println("The saved carbon emissions are " + result);
        return result;
    }

}

package generators;

import misc.Utils;

/**
 * Created by reda on 16/09/16.
 */

public class NormalGenerator extends IntegerGenerator {

    double mean;
    double sd;

    public NormalGenerator(double mean, double sd)
    {
        this.mean = mean;
        this.sd = sd;
    }

    /**
     * Return the next value as an int. When overriding this method, be sure to call setLastString() properly, or the lastString() call won't work.
     */
    @Override
    public int nextInt() {
        double r = Math.sqrt(-2 * Math.log(Utils.random().nextDouble()));
        double theta = 2 * Math.PI * Utils.random().nextDouble();
        return (int)(mean + sd * r * Math.cos(theta));
    }

    /**
     * Return the expected value (mean) of the values this generator will return.
     */
    @Override
    public double mean() {
        return mean;
    }
}

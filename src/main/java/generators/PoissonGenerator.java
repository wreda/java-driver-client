package generators;

import misc.Utils;

/**
 * Created by reda on 15/09/16.
 */

public class PoissonGenerator extends IntegerGenerator{

    //Poisson distribution parameters
    private double _lambda;


    /**
     * Creates a generator that will return integers based on a poisson process
     */
    public PoissonGenerator(double lambda)
    {
        _lambda = lambda;
    }

    @Override
    public int nextInt()
    {
        double L = Math.exp(-_lambda);
        double p = 1.0;
        int k = 0;

        do {
            k++;
            p *= Utils.random().nextDouble();
        } while (p > L);

        return k - 1;
    }

    @Override
    public double mean() {
        throw new UnsupportedOperationException();
    }
}
package generators;

/**
 * Created by reda on 16/09/16.
 */
public class ConstantGenerator extends IntegerGenerator {

    int constant;

    public ConstantGenerator(int constant)
    {
        this.constant = constant;
    }

    /**
     * Return the next value as an int. When overriding this method, be sure to call setLastString() properly, or the lastString() call won't work.
     */
    @Override
    public int nextInt() {
        return constant;
    }

    /**
     * Return the expected value (mean) of the values this generator will return.
     */
    @Override
    public double mean() {
        return 0;
    }
}

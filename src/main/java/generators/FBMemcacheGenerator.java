/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package generators;

import java.util.HashMap;
import java.util.Map;
import misc.Utils;

/**
 * Generates value sizes (in bytes) based on the "Workload Analysis of a Large-Scale Key-Value Store" paper.
 */
public class FBMemcacheGenerator extends IntegerGenerator
{
    // Generalized pareto distribution parameters
    private final double _mu=0;
    private final double _sigma=214.476;
    private final double _ksi=0.348238;

    // Facebook memcache paper's hardcoded probabilities for 1->14 bytes
    private Map<Integer, Double> distr_1_14;
    private double p_1_14;

    /**
     * Creates a generator that will return integers based on a pareto distribution initialized with params from the paper
     */
    public FBMemcacheGenerator()
    {
        //first 14 probabilities of value sizes for the FB Memcache distribution
        distr_1_14 = new HashMap<Integer, Double>();
        distr_1_14.put(1, 0.00583);
        distr_1_14.put(2, 0.17820);
        distr_1_14.put(3, 0.09239);
        distr_1_14.put(4, 0.00018);
        distr_1_14.put(5, 0.02740);
        distr_1_14.put(6, 0.00065);
        distr_1_14.put(7, 0.00606);
        distr_1_14.put(8, 0.00023);
        distr_1_14.put(9, 0.00837);
        distr_1_14.put(10, 0.00837);
        distr_1_14.put(11, 0.08989);
        distr_1_14.put(12, 0.00092);
        distr_1_14.put(13, 0.00326);
        distr_1_14.put(14, 0.01980);

        p_1_14 = 0.0;
        for (double d : distr_1_14.values()) {
            p_1_14 += d;
        }
    }

    @Override
    public int nextInt()
    {
        double r=0;
        double choice = Utils.random().nextDouble();
        if (choice > p_1_14)
        {
            while ((int)r <= 14)
                r = getParetoSample();
        }
        else
        {
            choice = Utils.random().nextDouble();
            double f_x = 0;
            for (int i : distr_1_14.keySet())
            {
                f_x += (distr_1_14.get(i)/p_1_14);
                if(choice <= f_x)
                {
                    r = i;
                    break;
                }
            }
        }
        setLastInt((int)r);
        return (int)r;
    }

    private double getParetoSample() {
        return _mu + _sigma * (Math.pow(Utils.random().nextDouble(), -_ksi) - 1) / _ksi;
    }

    @Override
    public double mean() {
        throw new UnsupportedOperationException();
    }
}

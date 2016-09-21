import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.*;

/**
 * Created by reda on 20/09/16.
 */

public class BatchPercentileTracker implements LatencyTracker {
    private static final Logger logger = LoggerFactory.getLogger(BatchPercentileTracker.class);

    private final int opCount;
    private final long highestTrackableLatencyMillis;
    private final int numberOfSignificantValueDigits;
    private final int minRecordedValues;
    private Histogram finalHistogram = null;

    private volatile Cluster cluster;

    private int opsCounter=0;
    private long lastUpdateTS;

    Pattern pattern = Pattern.compile("(?<=IN \\()(.*)(?=\\) LIMIT)");

    // The "live" recorders: this is where we store the latencies received from the cluster
    private final ConcurrentMap<Object, Recorder> recorders;

    /**
     * Builds a new instance.
     *
     * @see Builder
     */
    protected BatchPercentileTracker(int opCount,
                                      long highestTrackableLatencyMillis,
                                      int numberOfSignificantValueDigits,
                                      int minRecordedValues) {
        this.opCount = opCount;
        this.highestTrackableLatencyMillis = highestTrackableLatencyMillis;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.minRecordedValues = minRecordedValues;
        this.recorders = new ConcurrentHashMap<Object, Recorder>();
    }

    /**
     * Computes a key used to categorize measurements. Measurements with the same key will be recorded in the same
     * histogram.
     * <p/>
     * It's recommended to keep the number of distinct keys low, in order to limit the memory footprint of the
     * histograms.
     *
     * @param host      the host that was queried.
     * @param statement the statement that was executed.
     * @param exception if the query failed, the corresponding exception.
     * @return the key.
     */

    protected Integer computeKey(Host host, Statement statement, Exception exception) {
        Matcher m = this.pattern.matcher(statement.toString());
        if(m.find()) {
            String keys = m.group(1);
            String[] x = keys.split(",");
            return x.length;
        }
        else
            return null;
    }

    @Override
    public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
        this.lastUpdateTS = System.nanoTime();
        if (!include(host, statement, exception))
            return;
        this.opsCounter++;
        long latencyUs = NANOSECONDS.toMicros(newLatencyNanos);
        try {
            Recorder recorder = getRecorder(host, statement, exception);
            if (recorder != null)
                recorder.recordValue(latencyUs);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.warn("Got request with latency of {} us, which exceeds the configured maximum trackable value {}",
                    latencyUs, MILLISECONDS.toMicros(highestTrackableLatencyMillis));
        }
    }

    /**
     * Returns the request latency at a given percentile.
     *
     * @param host       the host (if this is relevant in the way percentiles are categorized).
     * @param statement  the statement (if this is relevant in the way percentiles are categorized).
     * @param exception  the exception (if this is relevant in the way percentiles are categorized).
     * @param percentile the percentile (for example, {@code 99.0} for the 99th percentile).
     * @return the latency (in milliseconds) at the given percentile, or a negative value if it's not available yet.
     * @see #computeKey(Host, Statement, Exception)
     */
    public long getLatencyAtPercentile(Host host, Statement statement, Exception exception, double percentile) {
        checkArgument(percentile >= 0.0 && percentile < 100,
                "percentile must be between 0.0 and 100 (was %s)", percentile);
        Histogram histogram = getLastIntervalHistogram(host, statement, exception);

        if(this.finalHistogram == null)
            this.finalHistogram = histogram;

        if (this.finalHistogram == null || this.finalHistogram.getTotalCount() < opsCounter) {
            System.out.println("total count " + this.finalHistogram.getTotalCount());
            System.out.println(this.finalHistogram);
            return -1;
        }

        return this.finalHistogram.getValueAtPercentile(percentile);
    }

    public Map<Object, Double> getAllLatenciesAtPercentile(double percentile)
    {
        Map<Object, Double> values = new HashMap<Object, Double>();
        for(Object i: recorders.keySet())
        {
            double value = recorders.get(i).getIntervalHistogram().getValueAtPercentile(percentile);
            values.put(i, value);
        }
        return values;
    }

    public boolean isRunComplete()
    {
        if(this.opCount + 1 == this.opsCounter)
            return true;
        else
            return false;
    }

    public long getLastUpdateTS()
    {
        return this.lastUpdateTS;
    }

    public int getOpsCount()
    {
        return this.opsCounter-1;
    }

    private Recorder getRecorder(Host host, Statement statement, Exception exception) {
        Object key = computeKey(host, statement, exception);
        if (key == null)
            return null;

        Recorder recorder = recorders.get(key);
        if (recorder == null) {
            recorder = new Recorder(highestTrackableLatencyMillis, numberOfSignificantValueDigits);
            Recorder old = recorders.putIfAbsent(key, recorder);
            if (old != null) {
                // We got beaten at creating the recorder, use the actual instance and discard ours
                recorder = old;
            }
        }
        return recorder;
    }

    /**
     * @return null if no histogram is available yet (no entries recorded, or not for long enough)
     */
    private Histogram getLastIntervalHistogram(Host host, Statement statement, Exception exception) {
        Object key = computeKey(host, statement, exception);

        if (key == null) {
            return null;
        }

        Recorder recorder = recorders.get(key);
        Histogram histogram = recorder.getIntervalHistogram();

        return histogram;
    }

    /**
     * @return null if no histogram is available yet (no entries recorded, or not for long enough)
     */
    private Histogram getLastIntervalHistogram(Host host, Object key, Exception exception) {
        if (key == null) {
            return null;
        }

        Recorder recorder = recorders.get(key);
        Histogram histogram = recorder.getIntervalHistogram();

        return histogram;
    }

    /**
     * A histogram and the timestamp at which it was retrieved.
     * The data is only relevant for (timestamp + intervalMs); after that, the histogram is stale and we want to
     * retrieve a new one.
     */
    static class CachedHistogram {
        final ListenableFuture<Histogram> histogram;
        final long timestamp;

        CachedHistogram(ListenableFuture<Histogram> histogram) {
            this.histogram = histogram;
            this.timestamp = System.currentTimeMillis();
        }

        static CachedHistogram empty() {
            return new CachedHistogram(Futures.<Histogram>immediateFuture(null));
        }
    }

    @Override
    public void onRegister(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void onUnregister(Cluster cluster) {
        // nothing by default
    }

    /**
     * Determines whether a particular measurement should be included.
     * <p/>
     * This is used to ignore measurements that could skew the statistics; for example, we typically want to ignore
     * invalid query errors because they have a very low latency and would make a given cluster/host appear faster than
     * it really is.
     *
     * @param host      the host that was queried.
     * @param statement the statement that was executed.
     * @param exception if the query failed, the corresponding exception.
     * @return whether the measurement should be included.
     */
    protected boolean include(Host host, Statement statement, Exception exception) {
        // query was successful: always consider
        if (exception == null)
            return true;
        // filter out "fast" errors
        // TODO this was copy/pasted from LatencyAwarePolicy, maybe it could be refactored as a shared method
        return !EXCLUDED_EXCEPTIONS.contains(exception.getClass());
    }

    /**
     * A set of DriverException subclasses that we should prevent from updating the host's score.
     * The intent behind it is to filter out "fast" errors: when a host replies with such errors,
     * it usually does so very quickly, because it did not involve any actual
     * coordination work. Such errors are not good indicators of the host's responsiveness,
     * and tend to make the host's score look better than it actually is.
     */
    private static final Set<Class<? extends Exception>> EXCLUDED_EXCEPTIONS = ImmutableSet.<Class<? extends Exception>>of(
            UnavailableException.class, // this is done via the snitch and is usually very fast
            OverloadedException.class,
            BootstrappingException.class,
            UnpreparedException.class,
            QueryValidationException.class // query validation also happens at early stages in the coordinator
    );

    /**
     * Returns a builder to create a new instance.
     *
     * @param highestTrackableLatencyMillis the highest expected latency. If a higher value is reported, it will be
     *                                      ignored and a warning will be logged. A good rule of thumb is to set it
     *                                      slightly higher than SocketOptions#getReadTimeoutMillis().
     * @return the builder.
     */
    public static Builder builder(int opCount, long highestTrackableLatencyMillis) {
        return new Builder(opCount, highestTrackableLatencyMillis);
    }

    /**
     * Helper class to build {@code BatchPercentileTracker} instances with a fluent interface.
     */
    public static class Builder {
        protected final int opCount;
        protected final long highestTrackableLatencyMillis;
        protected int numberOfSignificantValueDigits = 3;
        protected int minRecordedValues = 1000;

        Builder(int opCount, long highestTrackableLatencyMillis) {
            this.opCount = opCount;
            this.highestTrackableLatencyMillis = highestTrackableLatencyMillis;
        }

        protected Builder self() {
            return this;
        }

        public BatchPercentileTracker build() {
            return new BatchPercentileTracker(opCount, highestTrackableLatencyMillis, numberOfSignificantValueDigits,
                    minRecordedValues);
        }
    }

}


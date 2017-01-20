import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.querybuilder.Insert;
import generators.*;
import misc.ByteIterator;
import misc.RandomByteIterator;
import org.apache.commons.cli.*;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import misc.Utils;

public class AsyncClient {

    //Hardcoded values that should be adjusted based on cluster setup and underlying hardware
    double MEMORY_READ_SATURATION_INTERARRIVAL=300; //interarrival required to saturate system for memory reads
    double DISK_READ_SATURATION_INTERARRIVAL=100; //interarrival required to saturate system for disk reads

    int totalOps;
    int utilization;
    int interarrival; //interarrival time in Microseconds
    int ceilOps; //total number of rows
    Cluster cluster;
    Session session;
    CustomPercentileTracker tracker;
    BatchPercentileTracker batchTracker;
    boolean isRead; //workload is read-only
    FileGenerator filegen;
    IntegerGenerator bszGenerator;
    IntegerGenerator skwGenerator;
    IntegerGenerator valueGenerator;
    IntegerGenerator arrivalGenerator;
    String hostIP;
    boolean isTrace; //workload is generated using a trace file
    boolean isDebug = false;
    int seed = 46;

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        AsyncClient client = new AsyncClient();
        CommandLine commandLine = client.parseArgs(args);
        System.out.println(commandLine.getOptions());
        client.init(commandLine);
        client.setupCluster();
        client.runWorkload();
    }

    private void init(CommandLine cmd)
    {
        System.out.println(cmd);
        isDebug = cmd.hasOption("debug");
        isRead = cmd.hasOption("read");
        hostIP = cmd.getOptionValue("host", "127.0.0.1");
        totalOps = Integer.parseInt(cmd.getOptionValue("ops", "10000000"));
        utilization = Integer.parseInt(cmd.getOptionValue("util", "75"));
        isTrace = cmd.getOptionValue("workload", "trace").equals("trace");
        filegen = new FileGenerator(cmd.getOptionValue("trc",
                    "/Users/reda/git/cicero/trace-processing/third_simulatorTrace"));

        String bszDist = cmd.getOptionValue("bsz", "normal");
        String skewDist = cmd.getOptionValue("skw", "uniform");
        String valueDist = cmd.getOptionValue("value", "constant");
        String expScenario = cmd.getOptionValue("exp", "memory");

        int bszParam = Integer.parseInt(cmd.getOptionValue("bszp", "10"));
        int skwParam = Integer.parseInt(cmd.getOptionValue("skwp", "2"));
        int valueParam = Integer.parseInt(cmd.getOptionValue("valuep", "1000"));

        if(bszDist.equals("normal"))
            bszGenerator = new NormalGenerator(bszParam, 75);
        else if(bszDist.equals("zipfian"))
            bszGenerator = new ZipfianGenerator(1, 5000, bszParam); //How do we set upper/lower limits?
        else if(bszDist.equals("constant"))
            bszGenerator = new ConstantGenerator(bszParam);

        if(expScenario.equals("memory"))
        {
            //TODO Remove hardcoded values
            ceilOps = 100000; //row count is 100k
            double compensation = isTrace ? 8 : bszGenerator.mean();
            if(isRead)
                interarrival = (int)(MEMORY_READ_SATURATION_INTERARRIVAL / (((double)utilization)/100.0) / compensation);
            else
                interarrival = 800;
        }
        else
        {
            //TODO Remove hardcoded values
            ceilOps = 250000000; //row count is 250m
            double compensation = isTrace ? 8 : bszGenerator.mean();
            if(isRead)
                interarrival = (int) (DISK_READ_SATURATION_INTERARRIVAL /(((double)utilization)/100.0) / compensation);
            else
                interarrival = 800;
        }

        if(skewDist.equals("zipfian"))
            skwGenerator = new ZipfianGenerator(ceilOps, skwParam);
        else if(skewDist.equals("uniform"))
            skwGenerator = new UniformIntegerGenerator(1, ceilOps);

        if(valueDist.equals("fbpareto"))
            valueGenerator = new FBMemcacheGenerator();
        else if(valueDist.equals("constant"))
            valueGenerator = new ConstantGenerator(valueParam);

        arrivalGenerator = new ConstantGenerator(interarrival);
    }

    public void setupCluster() throws InterruptedException {
        tracker = CustomPercentileTracker
                .builder(totalOps, 250000000) //Set the highest trackable latency to an arbitrarily high value
                .build();
        batchTracker = BatchPercentileTracker
                .builder(totalOps, 250000000)
                .build();

        final long st_setup = System.nanoTime();

        cluster = Cluster.builder().addContactPoint(hostIP).withPoolingOptions(createPoolingOptions()).build();

        cluster.register(tracker);
        cluster.register(batchTracker);

        if(isDebug) {
            final LoadBalancingPolicy loadBalancingPolicy =
                    cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
            final PoolingOptions poolingOptions =
                    cluster.getConfiguration().getPoolingOptions();
            ScheduledExecutorService scheduled =
                    Executors.newScheduledThreadPool(1);
            scheduled.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    Session.State state = session.getState();
                    for (Host host : state.getConnectedHosts()) {
                        HostDistance distance = loadBalancingPolicy.distance(host);
                        int connections = state.getOpenConnections(host);
                        int inFlightQueries = state.getInFlightQueries(host);
                        System.out.printf("%s connections=%d, current load=%d, max load=%d%n",
                                host, connections, inFlightQueries, connections * poolingOptions.getMaxRequestsPerConnection(distance));
                    }
                }
            }, 5, 5, TimeUnit.SECONDS);
        }

//        tracker.onRegister(cluster);

//        cluster = Cluster.builder().addContactPoint("127.0.0.1").addContactPoint("127.0.0.2").build();
//        cluster.getConfiguration().getPoolingOptions().setConnectionsPerHost(HostDistance.LOCAL, 1, 1);
//        cluster.getConfiguration().getPoolingOptions().setMaxRequestsPerConnection(HostDistance.LOCAL, 5000);

        session = cluster.connect("ycsb");

        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

        for (Host discoveredHost : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    discoveredHost.getDatacenter(),
                    discoveredHost.getAddress(),
                    discoveredHost.getRack());
        }
        final long et_setup = System.nanoTime();
        System.out.println("Setup completed in " + (et_setup - st_setup) + " ns");
    }

    private PoolingOptions createPoolingOptions(){
        return new PoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 1) //Core connections for LOCAL hosts must be less than max (54 > 8) ???
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 1) //Core connections for LOCAL hosts must be less than max (54 > 8) ???

                .setCoreConnectionsPerHost(HostDistance.REMOTE, 1) //Core connections for REMOTE hosts must be less than max (54 > 2)
                .setMaxConnectionsPerHost(HostDistance.REMOTE, 1)

                .setMaxRequestsPerConnection(HostDistance.LOCAL, 5000) //Max simultaneous requests per connection for LOCAL hosts must be in the range (0, 128)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 5000); //Max simultaneous requests per connection for REMOTE hosts must be in the range (0, 128)

    }

    public void runWorkload() throws InterruptedException, ExecutionException, IOException {
        if(isRead)
            readData();
        else
            writeData();
    }

    public void readData() throws InterruptedException, ExecutionException, IOException {
        final long st_trans = System.nanoTime();
        double compensation = 0;
        //List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
        if(isTrace)
            System.out.println("Generating workload from trace file: " + filegen.getFilename());
        for(int i=0; i<totalOps; i++)
        {
            long st_asynccall = System.nanoTime();

            List<String> task = new ArrayList<String>();

            if(isTrace) {
                task = readMultiGetFromFile(filegen);
                for (int j = 0; j < task.size(); j++) {
                    long keyInt = Utils.hash(Integer.parseInt(task.get(j)));
                    keyInt = keyInt % ceilOps;

                    //int keyInt = Integer.parseInt(task.get(i));
                    String kname = buildKeyName(keyInt);
                    task.set(j, kname);
                }
            }
            else
            {
                int batchSize = bszGenerator.nextInt();
                if(batchSize <= 0)
                    batchSize = 1;
                for(int j=0; j<batchSize; j++)
                {
                    int k = skwGenerator.nextInt();
                    task.add(buildKeyName(k));
                }
            }

            Set<String> keys = new HashSet<String>(task);
            Set<String> fields = new HashSet<String>();
            fields.add("field0");
            Statement stmt = generateMultiGet("usertable", keys, fields);
            ResultSetFuture rsf = session.executeAsync(stmt);
            long et_asynccall = System.nanoTime();

            int delay = arrivalGenerator.nextInt();
            MICROSECONDS.sleep(Math.max(delay-NANOSECONDS.toMicros(et_asynccall-st_asynccall),0));

//            Futures.addCallback(rsf, new FutureCallback<ResultSet>() {
//                @Override
//                public void onSuccess(ResultSet resultSet) {
//                    //System.out.println("Updating tracker");
//                    //AsyncClient.this.incrementRequestsRecv();
//                }
//
//                @Override
//                public void onFailure(Throwable throwable) {
//                    System.out.printf("Failed with: %s\n", throwable);
//                }
//            });
            //results.add(rsf);
        }
        final long et_trans = System.nanoTime();

        System.out.println("Completed " + totalOps + " operations in " + (et_trans - st_trans)/1.0E9 + " seconds");

        while(!tracker.isRunComplete() && NANOSECONDS.toSeconds(System.nanoTime() - tracker.getLastUpdateTS()) < 10)
        {
            SECONDS.sleep(5);
        }

        //int notFoundCount = 0;
        //for(ResultSetFuture r: results) {
        //    if(r.get().all().size() == 0)
        //        notFoundCount += 1;
        //}

        System.out.println("Experiment completed in " + (System.nanoTime() - st_trans)/1.0E9 + " seconds");
        System.out.println("[MULTIGET-SUCCESS] Count: " + tracker.getOpsCount());
        if(!tracker.isRunComplete())
            System.out.println("[MULTIGET-FAILURE] Count: " + (totalOps - tracker.getOpsCount()));

        //if(notFoundCount>0)
        //    System.out.println("[WARNING] " + notFoundCount + " successful requests returned an empty response");

        double latency10Perc = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), null, null, 10);
        double latency20Perc = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), null, null, 20);
        double latency30Perc = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), null, null, 30);
        double latency40Perc = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), null, null, 40);
        double latencyMedian = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), null, null, 50);
        double latency95Perc = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), null, null, 95);
        double latency99Perc = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), null, null, 99);

        session.close();
        cluster.close();
        System.out.println("[MULTIGET] 10th Percentile Latency (us): " + latency10Perc);
        System.out.println("[MULTIGET] 20th Percentile Latency (us): " + latency20Perc);
        System.out.println("[MULTIGET] 30th Percentile Latency (us): " + latency30Perc);
        System.out.println("[MULTIGET] 40th Percentile Latency (us): " + latency40Perc);
        System.out.println("[MULTIGET] 50th Percentile Latency (us): " + latencyMedian);
        System.out.println("[MULTIGET] 95th Percentile Latency (us): " + latency95Perc);
        System.out.println("[MULTIGET] 99th Percentile Latency (us): " + latency99Perc);
        System.out.println("Throughput: " + tracker.getOpsCount()/NANOSECONDS.toSeconds(tracker.getLastUpdateTS()
                            - st_trans) + " reqs/sec");
        System.out.println("Sending rate: " + totalOps/NANOSECONDS.toSeconds(et_trans - st_trans) + " reqs/sec");

        String eol = System.getProperty("line.separator");
        try (Writer writer = new FileWriter("batchStats.csv")) {
            writer.append("size,latency").append(eol);
            for (Map.Entry<Object, Double> entry : batchTracker.getAllLatenciesAtPercentile(50).entrySet()) {
                writer.append(entry.getKey().toString())
                        .append(',')
                        .append(entry.getValue().toString())
                        .append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }

        System.out.println("All done");
    }

    public void writeData() throws InterruptedException
    {
        final long st_trans = System.nanoTime();
        for(int i=0; i<ceilOps; i++)
        {
            long st_asynccall = System.nanoTime();
            String kname = buildKeyName(i);
            List<String> fields = new ArrayList<String>();
            fields.add("field0");
            HashMap<String, ByteIterator> values = buildValues(fields);
            Statement stmt = generateInsert("usertable", kname, values);
            ResultSetFuture rsf = session.executeAsync(stmt);
            long et_asynccall = System.nanoTime();

            int delay = arrivalGenerator.nextInt();
            MICROSECONDS.sleep(Math.min(delay-NANOSECONDS.toMicros(et_asynccall-st_asynccall),0));
        }
        final long et_trans = System.nanoTime();
        double duration = (et_trans - st_trans)/1.0E9;
        System.out.println("Completed " + ceilOps + " operations in " + duration + " seconds");

        while(!tracker.isRunComplete() && NANOSECONDS.toSeconds(System.nanoTime() - tracker.getLastUpdateTS()) < 10)
        {
            SECONDS.sleep(5);
        }

        System.out.println("[WRITE-SUCCESS] Count: " + tracker.getOpsCount());
        if(!tracker.isRunComplete())
            System.out.println("[WRITE-FAILURE] Count: " + (ceilOps - tracker.getOpsCount()));

        double latencyMedian = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), null, null, 50);
        double latency95Perc = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), null, null, 95);
        double latency99Perc = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), null, null, 99);

        session.close();
        cluster.close();
        System.out.println("[WRITE] Median Latency (us): " + latencyMedian);
        System.out.println("[WRITE] 95th Percentile Latency (us): " + latency95Perc);
        System.out.println("[WRITE] 99th Percentile Latency (us): " + latency99Perc);
        System.out.println("Throughput: " + tracker.getOpsCount()/NANOSECONDS.toSeconds(tracker.getLastUpdateTS()
                - st_trans) + " reqs/sec");
        System.out.println("Sending rate: " + ceilOps/NANOSECONDS.toSeconds(et_trans - st_trans) + " reqs/sec");
        System.out.println("All done");
    }
    
    public static Statement generateMultiGet(String table, Set<String> keys, Set<String> fields)
     {
        final long st = System.nanoTime();
        Statement stmt;
        Select.Builder selectBuilder;

        if (fields == null) {
            selectBuilder = QueryBuilder.select().all();
        }
        else {
            selectBuilder = QueryBuilder.select();
            for (String col : fields) {
                ((Select.Selection) selectBuilder).column(col);
            }
        }

        stmt = selectBuilder.from(table).where(QueryBuilder.in("y_id", keys.toArray())).limit(keys.size());

        //System.out.println(stmt.toString());
        stmt.setConsistencyLevel(ConsistencyLevel.valueOf("ONE"));
        stmt.setFetchSize(Integer.MAX_VALUE);
        return stmt;
    }

    public static Statement generateInsert(String table, String key, HashMap<String, ByteIterator> values) {
        Insert insertStmt = QueryBuilder.insertInto(table);

        // Add key
        insertStmt.value("y_id", key);

        // Add fields
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            Object value;
            ByteIterator byteIterator = entry.getValue();
            value = byteIterator.toString();

            insertStmt.value(entry.getKey(), value);
        }

        insertStmt.setConsistencyLevel(ConsistencyLevel.valueOf("ONE"));

        return insertStmt;
    }

    /**
     * Reads and parses next line in the workload trace
     * @throws UnsupportedOperationException
     */
    private static List<String> readMultiGetFromFile(FileGenerator filegen) throws UnsupportedOperationException
    {
        String line = filegen.nextString();
        if(line == null)
            return null;
        List<String> task;
        line.replaceAll("\n", "");
        if(line.startsWith("R "))
            task = new ArrayList(Arrays.asList(line.split(" ")));
        else
            throw new UnsupportedOperationException();
        task.remove(0); //remove R
        task.remove(task.size()-1); //remove interarrival modifier (no longer used)
        return task;
    }

    public static String buildKeyName(long keynum) {
        return "user" + keynum;
    }

    /**
     * Builds values for all fields.
     */
    private HashMap<String, ByteIterator> buildValues(List<String> fields) {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

        for (String fieldkey : fields) {
            ByteIterator data;

            // fill with random data
            data = new RandomByteIterator(valueGenerator.nextInt());
            values.put(fieldkey, data);
        }
        return values;
    }

    public static CommandLine parseArgs(String[] args)
    {
        CommandLine commandLine;
        Option option_A = Option.builder("read")
                .desc("Read rows from database")
                .build();
        Option option_B = Option.builder("write")
                .desc("Populate database with rows")
                .build();
        Option option_C = Option.builder("bsz")
                .longOpt("batch-distr")
                .desc("Batch size distribution for read workloads. Can be 'zipfian', 'constant', or 'normal'")
                .hasArg()
                .type(String.class)
                .argName("type")
                .build();
        Option option_D = Option.builder("skw")
                .longOpt("skew-distr")
                .desc("Access skew distribution for read workloads. Can be 'zipfian' or 'uniform'")
                .hasArg()
                .type(String.class)
                .argName("type")
                .build();
        Option option_E = Option.builder("trc")
                .longOpt("trace-file")
                .desc("Path to the trace file for the batch-distr")
                .hasArg()
                .type(String.class)
                .argName("path")
                .build();
        Option option_F = Option.builder("exp")
                .desc("The experiment scenario - can be either 'memory' or 'disk' intensive")
                .hasArg()
                .type(String.class)
                .argName("type")
                .build();
        Option option_G = Option.builder("util")
                .longOpt("utilization")
                .desc("Target system utilization for read workloads")
                .hasArg()
                .type(Integer.class)
                .argName("number")
                .build();
        Option option_H = Option.builder("ops")
                .longOpt("operations-count")
                .desc("Total number of operations to generate for read workloads")
                .hasArg()
                .type(Integer.class)
                .argName("number")
                .build();
        Option option_I = Option.builder("value")
                .longOpt("value-distr")
                .desc("Value size distribution for insertions")
                .hasArg()
                .argName("type")
                .build();
        Option option_J = Option.builder("workload")
                .desc("Describes the type of workload. Can be either 'trace' or 'synthetic'")
                .hasArg()
                .argName("type")
                .build();
        Option option_K = Option.builder("host")
                .desc("IP address for one of the nodes in the cassandra cluster")
                .hasArg()
                .argName("ip")
                .build();
        Option option_L = Option.builder("bszp")
                .desc("Batch size distribution parameter")
                .hasArg()
                .type(Integer.class)
                .argName("ip")
                .build();
        Option option_M = Option.builder("skwp")
                .desc("Skew distribution parameter")
                .hasArg()
                .type(Integer.class)
                .argName("ip")
                .build();
        Option option_N = Option.builder("valuep")
                .desc("Value size distribution parameter")
                .hasArg()
                .type(Integer.class)
                .argName("ip")
                .build();
        Option option_O = Option.builder("debug")
                .desc("Enable debugging mode")
                .build();

        Options options = new Options();
        options.addOption(option_A);
        options.addOption(option_B);
        options.addOption(option_C);
        options.addOption(option_D);
        options.addOption(option_E);
        options.addOption(option_F);
        options.addOption(option_G);
        options.addOption(option_H);
        options.addOption(option_I);
        options.addOption(option_J);
        options.addOption(option_K);
        options.addOption(option_L);
        options.addOption(option_M);
        options.addOption(option_N);
        options.addOption(option_O);

        CommandLineParser parser = new DefaultParser();
        try
        {
            commandLine = parser.parse(options, args);
        }
        catch (ParseException exception)
        {
            System.out.print("Parse error: ");
            System.out.println(exception.getMessage());
            return null;
        }

        return commandLine;
    }
}

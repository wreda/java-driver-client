import java.util.*;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.*;
import org.apache.commons.math3.distribution.NormalDistribution;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import generators.FileGenerator;
import misc.Utils;

public class AsyncClient {
    static long invocation = 0;

    public static void main(String[] args) throws InterruptedException {
        int totalOps = 100000;
        int interarrival = 10; //Interarrival time in Microseconds
        int ceilOps = 100000;
        FileGenerator filegen = new FileGenerator("/Users/reda/git/cicero/trace-processing/third_simulatorTrace");
        int seed = 46;
        Cluster cluster;
        Session session;
        CustomPercentileTracker tracker = CustomPercentileTracker
                .builder(totalOps, 15000)
                .build();
        List<ResultSetFuture> results = new ArrayList<>(totalOps);
        NormalDistribution dist = new NormalDistribution(8.0, 2.0);
        Random rng = new Random(seed);
        dist.reseedRandomGenerator(seed);
        
        final long st_setup = System.nanoTime();
     // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        cluster.getConfiguration().getPoolingOptions().setConnectionsPerHost(HostDistance.LOCAL, 100, 100);
        cluster.getConfiguration().getPoolingOptions().setMaxRequestsPerConnection(HostDistance.LOCAL, 50);
        cluster.register(tracker);
        tracker.onRegister(cluster);

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
        
        final long st_trans = System.nanoTime();
        Statement globalStmt = null;
        for(int i=0; i<totalOps; i++)
        {
            long st_asynccall = System.nanoTime();
/*            int batchSize = (int) Math.round(dist.sample());
            if(batchSize <= 0)
                batchSize = 1;
            Set<String> keys = new HashSet<String>();
            for(int j=0; j<batchSize; j++)
            {
                int k = rng.nextInt(ceilOps); // Number of IDs in usertable is ceilOps
                keys.add(buildKeyName(k);
            }*/

            List<String> task = readMultiGetFromFile(filegen);
            for (int j = 0; j < task.size(); j++) {
                long keyInt = Utils.hash(Integer.parseInt(task.get(j)));
                keyInt = keyInt%100000;

                //int keyInt = Integer.parseInt(task.get(i));
                String kname = buildKeyName(keyInt);
                task.set(j, kname);
            }

            Set<String> keys = new HashSet<String>(task);
            Set<String> fields = new HashSet<String>();
            fields.add("field0");
            Statement stmt = generateMultiGet("usertable", keys, fields, session);
            ResultSetFuture rsf = session.executeAsync(stmt);
            long et_asynccall = System.nanoTime();

            MICROSECONDS.sleep(Math.min(interarrival-NANOSECONDS.toMicros(et_asynccall-st_asynccall),0));

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
        double duration = (et_trans - st_trans)/1.0E9;
        System.out.println("Completed " + totalOps + " operations in " + duration + " seconds");

        while(!tracker.isRunComplete() && NANOSECONDS.toSeconds(System.nanoTime() - tracker.getLastUpdateTS()) < 10)
        {
            SECONDS.sleep(5);
        }

        System.out.println("[MULTIGET-SUCCESS] Count: " + tracker.getOpsCount());
        if(!tracker.isRunComplete())
            System.out.println("[MULTIGET-FAILURE] Count: " + (totalOps - tracker.getOpsCount()));

        double latencyMedian = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), globalStmt, null, 50);
        double latency95Perc = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), globalStmt, null, 95);
        double latency99Perc = tracker.getLatencyAtPercentile(cluster.getMetadata().getAllHosts().iterator().next(), globalStmt, null, 99);

        session.close();
        cluster.close();
        System.out.println("[MULTIGET] Median Latency (us):" + latencyMedian);
        System.out.println("[MULTIGET] 95th Percentile Latency (us):" + latency95Perc);
        System.out.println("[MULTIGET] 99th Percentile Latency (us):" + latency99Perc);
        System.out.println("All done");
    }
    
    public static Statement generateMultiGet(String table, Set<String> keys, Set<String> fields, Session session)
     {
        final long st = System.nanoTime();
        Statement stmt;
        Select.Builder selectBuilder;

        invocation += 1;
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
        return stmt;
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
}

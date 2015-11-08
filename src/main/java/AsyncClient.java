import java.util.*;

import org.apache.commons.math3.distribution.NormalDistribution;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

public class AsyncClient {
    static long invocation = 0;

    public static void main(String[] args) {
        int totalOps = 1000000;
        int seed = 46;
        Cluster cluster;
        Session session;
        List<ResultSetFuture> results = new ArrayList<>(totalOps);
        NormalDistribution dist = new NormalDistribution(8.0, 2.0);
        Random rng = new Random(seed);
        dist.reseedRandomGenerator(seed);
        
        final long st_setup = System.nanoTime();
     // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder().addContactPoint("192.168.100.4").build();
        cluster.getConfiguration().getPoolingOptions().setConnectionsPerHost(HostDistance.LOCAL, 100, 100);
        cluster.getConfiguration().getPoolingOptions().setMaxRequestsPerConnection(HostDistance.LOCAL, 50);

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
        System.out.println("Setup completed in " + (et_setup - st_setup) + "ns");       
        
        final long st = System.nanoTime();
        for(int i=0; i<totalOps; i++)
        {
	    //int batchSize = 1;
            int batchSize = (int) Math.round(dist.sample());
            if(batchSize <= 0)
                batchSize = 1;
            Set<String> keys = new HashSet<String>();
            for(int j=0; j<batchSize; j++)
            {
                int k = rng.nextInt(100000); // Number of IDs in usertable is 100000
                keys.add("user" + Integer.toString(k));
            }
            Set<String> fields = new HashSet<String>();
            fields.add("field0");
            ResultSetFuture rsf = readMulti_nonBlocking("usertable", keys, fields, session);
            results.add(rsf);
        }
        final long et = System.nanoTime();
        double duration = (et - st)/1.0E9;
        System.out.println("Completed " + totalOps + " operations in " + duration + " seconds");
        int count = 1;
        for (ResultSetFuture rsf : results) {
            ResultSet rs = rsf.getUninterruptibly();
            System.out.println("RS" + count + " latency: " + rs.latency() + " ns");
            count++;
        }
        session.close();
        cluster.close();
        System.out.println("All done");
    }
    
    public static ResultSetFuture readMulti_nonBlocking(String table, Set<String> keys, Set<String> fields, Session session)
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
        System.out.println(stmt.toString());
        stmt.setConsistencyLevel(ConsistencyLevel.valueOf("ONE"));

        long test1 = System.nanoTime();
        ResultSetFuture rs = session.executeAsync(stmt);
        long test2 = System.nanoTime();
        long timeElapsed = test2 - test1;
        System.out.println("Time to execute task " + invocation + "  = " + timeElapsed + " ns" + ". current thread = " + java.lang.Thread.currentThread() ); // + "Done: " + rs.isDone());
//        Futures.addCallback(rs,
//                new FutureCallback<ResultSet>() {
//                    public void onSuccess(ResultSet result) {
//                        long en=System.nanoTime();
//                        //while (!result.isExhausted()) {
//                        //    Row row = result.one(); //For now, we do nothing with the returned results
//                        //}
//                        System.out.println("Received rs" + ". current thread = " + java.lang.Thread.currentThread());
//                    }
//
//                    public void onFailure(Throwable t) {
//                        System.out.println("Error reading query: " + t.getMessage());
//                        long en=System.nanoTime();
//                    }
//                },
//                MoreExecutors.sameThreadExecutor()
//         );
         return rs;
    }
}

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.commons.math3.distribution.NormalDistribution;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

public class AsyncClient {
    
    public static void main(String[] args) {
        
        int totalOps = 1000000;
        int seed = 46;
        Cluster cluster;
        Session session;
        
     // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("ycsb");
        
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

        for (Host discoveredHost : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                discoveredHost.getDatacenter(),
                discoveredHost.getAddress(),
                discoveredHost.getRack());
        }
        
        NormalDistribution dist = new NormalDistribution(5.0, 2.0);
        Random rng = new Random(seed);
        dist.reseedRandomGenerator(seed);
        
        final long st = System.nanoTime();
        for(int i=0; i<totalOps; i++)
        {
            int batchSize = (int) Math.round(1);
            if(batchSize <= 0)
                batchSize = 1;
            Set<String> keys = new HashSet<String>();
            for(int j=0; j<batchSize; j++)
            {
                int k = Integer.toString(rng.nextInt()).hashCode();
                k = k % totalOps;
                keys.add(Integer.toString(k));
            }
            Set<String> fields = new HashSet<String>();
            fields.add("field0");
            readMulti_nonBlocking("usertable", keys, fields, session);
        }
        final long et = System.nanoTime();
        double duration = (et - st)/1.0E9;
        System.out.println("Completed " + totalOps + " operations in " + duration + " seconds");
    }
    
    public static void readMulti_nonBlocking(String table, Set<String> keys, Set<String> fields, Session session)
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
        stmt.setConsistencyLevel(ConsistencyLevel.valueOf("ONE"));

        long test1 = System.nanoTime();
        ResultSetFuture rs = session.executeAsync(stmt);
        long test2 = System.nanoTime();
        long timeElapsed = test2 - test1;
        //System.out.println("Time to execute task = " + timeElapsed + " ns" + "Done: " + rs.isDone());
//        Futures.addCallback(rs,
//                new FutureCallback<ResultSet>() {
//                    @Override public void onSuccess(ResultSet result) {
//                        long en=System.nanoTime();
//                        //while (!result.isExhausted()) {
//                        //    Row row = result.one(); //For now, we do nothing with the returned results
//                        //}
//                        dbWrapper.measure("READMULTI",ist, st, en);
//                        dbWrapper._measurements.reportReturnCode("READMULTI",OK);
//                    }
//             
//                    @Override public void onFailure(Throwable t) {
//                        System.out.println("Error reading query: " + t.getMessage());
//                        long en=System.nanoTime();
//                        dbWrapper.measure("READMULTI",ist, st, en);
//                        dbWrapper._measurements.reportReturnCode("READMULTI",ERR);
//                    }
//                },
//                MoreExecutors.sameThreadExecutor()
//         );
    }
}

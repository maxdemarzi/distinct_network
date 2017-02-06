package com.maxdemarzi;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class Benchmarks {

    private GraphDatabaseService db;

    @Setup(Level.Invocation )
    public void prepare() throws IOException, KernelException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Procedures proceduresService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class);
        proceduresService.registerProcedure(DistinctNetwork.class);

        try ( Transaction tx = db.beginTx() ) {
            db.schema().constraintFor(Label.label("Customer")).assertPropertyIsUnique("CustomerID");
            tx.success();
        }
        try ( Transaction tx = db.beginTx() ) {
            db.execute("WITH ['Jennifer','Michelle','Tanya','Julie','Christie','Sophie','Amanda','Khloe','Sarah','Kaylee'] AS names \n" +
                    "FOREACH (r IN range(0,100000) | CREATE (:Customer {CustomerID:names[r % size(names)]+r}))");

            db.execute("MATCH (u1:Customer),(u2:Customer)\n" +
                    "WITH u1,u2\n" +
                    "LIMIT 10000000\n" +
                    "WHERE rand() < 0.1\n" +
                    "CREATE (u1)-[:FRIEND_OF]->(u2);");
            tx.success();
        }
    }


    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureDistinctNetwork() throws IOException {
        try ( Transaction tx = db.beginTx() ) {
            db.execute("MATCH (c:Customer {CustomerID:'Jennifer0'}) CALL com.maxdemarzi.distinct_network(c) YIELD value RETURN value");
            tx.success();
        }
    }

}

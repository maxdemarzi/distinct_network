package com.maxdemarzi;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.neo4j.storageengine.api.RelationshipItem;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Stream;

public class DistinctNetwork {
    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    @Context
    public GraphDatabaseAPI dbAPI;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @Description("com.maxdemarzi.distinct_network(node) | Find Distinct Customer Ids 4 levels from network")
    @Procedure(name = "com.maxdemarzi.distinct_network", mode = Mode.READ)
    public Stream<StringResult> GetDistinctNetwork(@Name("from") Node start) throws EntityNotFoundException {
        ThreadToStatementContextBridge ctx = dbAPI.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
        ReadOperations ops = ctx.get().readOperations();
        int propertyCustomerIDkey = ops.propertyKeyGetForName("CustomerID");
        ArrayList<StringResult> results = new ArrayList<>();
        RoaringBitmap[] seen = new RoaringBitmap[] { new RoaringBitmap(), new RoaringBitmap(), new RoaringBitmap(), new RoaringBitmap(),
                new RoaringBitmap() };
        seen[0].add((int)start.getId());

        RelationshipIterator relationshipIterator = ops.nodeGetRelationships(start.getId(), Direction.OUTGOING);
        Cursor<RelationshipItem> c;
        Iterator<Integer> iterator;

        while (relationshipIterator.hasNext()) {
            c = ops.relationshipCursor(relationshipIterator.next());
            c.next();
            seen[1].add((int)c.get().endNode());
        }

        // Remove user from set just in case
        seen[1].andNot(seen[0]);

        // Level 2
        iterator = seen[1].iterator();
        while (iterator.hasNext()) {
            relationshipIterator = ops.nodeGetRelationships((long)iterator.next(), Direction.OUTGOING);
            while (relationshipIterator.hasNext()) {
                c = ops.relationshipCursor(relationshipIterator.next());
                c.next();
                seen[2].add((int)c.get().endNode());
            }
        }
        // Remove user and first level from level 2
        seen[2].andNot(seen[1]);
        seen[2].andNot(seen[0]);

        // Level 3

        iterator = seen[2].iterator();
        while (iterator.hasNext()) {
            relationshipIterator = ops.nodeGetRelationships(iterator.next(), Direction.OUTGOING);
            while (relationshipIterator.hasNext()) {
                c = ops.relationshipCursor(relationshipIterator.next());
                c.next();
                seen[3].add((int)c.get().endNode());
            }
        }
        // Remove user, first level and second level from level 3
        seen[3].andNot(seen[2]);
        seen[3].andNot(seen[1]);
        seen[3].andNot(seen[0]);

        // Level 4
        iterator = seen[3].iterator();
        while (iterator.hasNext()) {
            relationshipIterator = ops.nodeGetRelationships(iterator.next(), Direction.OUTGOING);
            while (relationshipIterator.hasNext()) {
                c = ops.relationshipCursor(relationshipIterator.next());
                c.next();
                seen[4].add((int)c.get().endNode());
            }
        }
        // Remove user, first level, second level and third level from level 4
        seen[4].andNot(seen[4]);
        seen[4].andNot(seen[2]);
        seen[4].andNot(seen[1]);
        seen[4].andNot(seen[0]);


        // Get Results
        iterator = seen[1].iterator();
        while (iterator.hasNext()) {
            results.add(new StringResult((String)ops.nodeGetProperty(iterator.next(), propertyCustomerIDkey)));
        }

        iterator = seen[2].iterator();
        while (iterator.hasNext()) {
            results.add(new StringResult((String)ops.nodeGetProperty(iterator.next(), propertyCustomerIDkey)));
        }

        iterator = seen[3].iterator();
        while (iterator.hasNext()) {
            results.add(new StringResult((String)ops.nodeGetProperty(iterator.next(), propertyCustomerIDkey)));
        }

        iterator = seen[4].iterator();
        while (iterator.hasNext()) {
            results.add(new StringResult((String)ops.nodeGetProperty(iterator.next(), propertyCustomerIDkey)));
        }

        return results.stream();
    }
}

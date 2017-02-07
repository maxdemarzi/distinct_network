package com.maxdemarzi;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class DistinctNetworkTest {
    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withProcedure(DistinctNetwork.class);

    @Test
    public void shouldGetDistinctNetwork() {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), QUERY);
        HashSet row = getResultRow(response);

        assertEquals(ANSWER_SET, row);
    }

    private static final HashMap<String, Object> QUERY = new HashMap<String, Object>(){{
        put("statements", new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("statement", "MATCH (c:Customer {CustomerID:'c1'}) CALL com.maxdemarzi.distinct_network2(c) YIELD value RETURN value");
            }});
        }});
    }};
    private static final String MODEL_STATEMENT =
            // 1->2->3->4
            // 1->5->3
            // 1->5->6
            "CREATE (c1:Customer {CustomerID:'c1'})" +
            "CREATE (c2:Customer {CustomerID:'c2'})" +
            "CREATE (c3:Customer {CustomerID:'c3'})" +
            "CREATE (c4:Customer {CustomerID:'c4'})" +
            "CREATE (c5:Customer {CustomerID:'c5'})" +
            "CREATE (c6:Customer {CustomerID:'c6'})" +
            "CREATE (c1)-[:FRIEND_OF]->(c2)" +
            "CREATE (c2)-[:FRIEND_OF]->(c3)" +
            "CREATE (c3)-[:FRIEND_OF]->(c4)" +
            "CREATE (c1)-[:FRIEND_OF]->(c5)" +
            "CREATE (c5)-[:FRIEND_OF]->(c3)" +
            "CREATE (c5)-[:FRIEND_OF]->(c6)";



    private static final HashSet<String> ANSWER_SET = new HashSet<String>(){{
        add("c2");
        add("c5");
        add("c3");
        add("c6");
        add("c4");
    }};


    static HashSet getResultRow(HTTP.Response response) {
        Map actual = response.content();
        ArrayList results = (ArrayList)actual.get("results");
        HashMap result = (HashMap)results.get(0);
        ArrayList<Map> data = (ArrayList)result.get("data");
        HashSet<String> values = new HashSet();
        data.forEach((value) -> values.add((String)((ArrayList) value.get("row")).get(0)));
        return values;
    }
}

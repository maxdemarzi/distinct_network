# distinct_network
Stored Procedure to get the distinct network of a node 4 levels out

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file,`target/procedures-1.0-SNAPSHOT.jar`,
that can be deployed in the `plugin` directory of your Neo4j instance.

    cp target/procedures-1.0-SNAPSHOT.jar neo4j-enterprise-3.1.0/plugins/.

If you don't have maven, you can look at the releases tab of this github repository and grab a pre-compiled jar.

Start Neo4j and log in.

Create sample users:

        WITH ['Jennifer','Michelle','Tanya','Julie','Christie','Sophie','Amanda','Khloe','Sarah','Kaylee'] AS names 
        FOREACH (r IN range(0,100000) | CREATE (:Customer {CustomerID:names[r % size(names)]+r}))
        
Connect those sample users:
        
        MATCH (u1:Customer),(u2:Customer)
        WITH u1,u2
        LIMIT 100000000
        WHERE rand() < 0.1
        CREATE (u1)-[:FRIEND_OF]->(u2);

Try the stored procedure:
        
        MATCH (c:Customer {CustomerID:'Jennifer0'}) CALL com.maxdemarzi.distinct_network(c) YIELD value RETURN value
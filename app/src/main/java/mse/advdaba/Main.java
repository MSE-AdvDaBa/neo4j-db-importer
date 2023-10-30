package mse.advdaba;

import org.neo4j.driver.*;

import java.util.*;

public class Main {
    public final static int MAX_NODES = Integer.parseInt(System.getenv("MAX_NODES"));
    public final static long START = System.currentTimeMillis();

    public static double elapsed() {
        return (double) (System.currentTimeMillis() - START) / 1000d;
    }

    public static void main(String[] args) {
        String jsonPath = System.getenv("JSON_FILE");
        System.out.println("Path to JSON file is " + jsonPath);
        System.out.println("Number of articles to consider is " + MAX_NODES);
        String neo4jIP = System.getenv("NEO4J_IP");
        System.out.println("IP address of neo4j server is " + neo4jIP);

        Driver driver = GraphDatabase.driver("bolt://" + neo4jIP + ":7687", AuthTokens.basic("neo4j", "testtest"));
        boolean connected = false;
        System.out.println("Trying to connect to db");
        do {
            try {
                driver.verifyConnectivity();
                connected = true;
            } catch (Exception e) {
                System.out.println("Failed to connect to db, retrying in 5 seconds");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException interruptedException) {
                    System.out.println("Interrupted");
                    System.exit(0);
                }
            }
        } while (!connected);
        System.out.println("Connected to db");
        try (Session session = driver.session()) {
            Transaction tx = session.beginTransaction();
            System.out.println("Deleting all nodes");
            tx.run("MATCH (n) DETACH DELETE n");
            tx.commit();
            tx = session.beginTransaction();
            tx.run("create constraint article_id IF NOT EXISTS for (a:Article) REQUIRE a._id is UNIQUE");
            tx.commit();
            tx = session.beginTransaction();
            tx.run("create constraint author_id IF NOT EXISTS for (a:Author) REQUIRE a._id is UNIQUE");
            tx.commit();
        }
        try (Session session = driver.session()) {
            readFile(jsonPath, session);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Failed to read file, shutting down");
            System.exit(2);
        }

        driver.close();
        System.out.println("Driver closed successfully");
        System.out.println("Finished in " + elapsed() + " seconds");
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Interrupted");
                System.exit(0);
            }
        }
    }

    public static void readFile(String jsonPath, Session session) {

        System.out.println("Inserting articles and authors of articles from jsonPath");

        session.run("""
                PROFILE
                CALL apoc.periodic.iterate(
                    "CALL apoc.load.jsonArray($jsonPath) YIELD value RETURN value",
                    "UNWIND value as article
                    WITH article
                    ORDER BY article.year DESC
                    LIMIT $maxNodes
                    UNWIND article.authors as author
                    WITH article, author WHERE author._id IS NOT NULL
                    MERGE (a:Author {_id:author._id})
                    ON CREATE SET a.name = author.name
                    MERGE (b:Article {_id:article._id})
                    ON CREATE SET b.title = article.title, b.year = article.year
                    MERGE (a)-[:AUTHORED]->(b)",
                    {batchSize:50,parallel:false, params:{jsonPath:$jsonPath, maxNodes:$maxNodes}}
                )
                """, Map.of("jsonPath", jsonPath, "maxNodes", MAX_NODES));

        System.out.println("Creating citations link");

        session.run("""
                PROFILE
                CALL apoc.periodic.iterate(
                "MATCH(a:Article) return a",
                "UNWIND a.cites as quote MERGE(b:Article {_id:quote}) CREATE(a)-[:CITES]->(b) REMOVE a.cites",
                {batchSize:50,parallel:false}
                )
                """);

        System.out.println("Waiting for pending operations to finish");
    }

    public static String sanitize(String input) {
        return input.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("'", "\\'");
    }
}
package mse.advdaba;
import org.neo4j.driver.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
        do {
            try {
                System.out.println("Trying to connect to db");
                driver.verifyConnectivity();
                connected = true;
            } catch (Exception e) {
                System.out.println("Failed to connect to db, retrying");
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
    }

    public static void readFile(String jsonPath, Session session) throws IOException {
        FileReader fr = new FileReader(jsonPath);
        BufferedReader br = new BufferedReader(fr);
        Map<String, List<String>> authors = new HashMap<>();
        int crtNode = 0;

        System.out.println("Inserting articles");
        session.run("""
                CALL apoc.periodic.iterate(
                    "LOAD CSV WITH HEADERS FROM 'file:///csv/articles.csv' as line FIELDTERMINATOR '§' return line",
                    "WITH line, apoc.convert.fromJsonList(line.cites) as cites CREATE(:Article {_id:line._id,title:line.title,cites:cites})",
                    {batchSize:50,parallel:false}
                )
                """);

        System.out.println("Inserting authors");

        session.run("""
                CALL apoc.periodic.iterate(
                    "LOAD CSV WITH HEADERS FROM 'file:///csv/authors.csv' as line FIELDTERMINATOR '§' return line",
                    "WITH line, apoc.convert.fromJsonList(line.authored) as authored CREATE(:Author {_id:line._id,name:line.name,authored:authored})",
                    {batchSize:50,parallel:false}
                )
                """);
        System.out.println("Creating links between authors and articles");

        session.run("""
                CALL apoc.periodic.iterate(
                    "MATCH(a:Author) RETURN a",
                    "UNWIND a.authored as authored MATCH(b:Article {_id:authored}) MERGE(a)-[:AUTHORED]->(b) REMOVE a.authored",
                    {batchSize:50,parallel:false}
                )
                """);
        System.out.println("Creating citations link");
        session.run("""
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
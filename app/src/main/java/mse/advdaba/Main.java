package mse.advdaba;

import com.google.gson.stream.JsonReader;

import org.neo4j.driver.*;

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
        System.out.println("Trying to connect to db");
        do {
            try {
                driver.verifyConnectivity();
                connected = true;
            } catch (Exception e) {
                System.out.println("Failed to connect to db, retrying in 2 seconds");
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

        // Read JSON file and store it in a Map using Gson library
        Map<String, Object> map = null;
        JsonReader reader = null;
        try {
            reader = new JsonReader(new FileReader(jsonPath));
            reader.beginArray();
            map = new HashMap<>();
            int i = 0;
            int inserted = 0;
            System.out.println("Creating articles");
            while (reader.hasNext()) {
                if (i >= MAX_NODES) {
                    System.out.println("Inserting articles " + inserted + " to " + (inserted + MAX_NODES));
                    insertArticles(map, session);
                    inserted += MAX_NODES;
                    map = new HashMap<>();
                    i = 0;
                } else {
                    reader.beginObject();
                    HashMap<String, Object> objectMap = new HashMap<>();
                    while (reader.hasNext()) {
                        String name = reader.nextName();
                        switch (name) {
                            case "_id":
                                objectMap.put("_id", reader.nextString());
                                break;
                            case "title":
                                objectMap.put("title", sanitize(reader.nextString()));
                                break;
                            case "year":
                                objectMap.put("year", reader.nextInt());
                                break;
                            default:
                                reader.skipValue();
                                break;
                        }
                    }
                    map.put(objectMap.get("_id").toString(), objectMap);
                    reader.endObject();
                    i++;
                }
            }
            System.out.println("Inserting articles " + inserted + " to " + (inserted + i));
            insertArticles(map, session);
            reader.endArray();
            reader.close();
        } catch (IOException e) {
            System.out.println("Failed to read file");
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("Finished creating articles");
    }

    public static void insertArticles(Map<String, Object> map, Session session) {
        System.out.println("Inserting articles");
        Transaction tx = session.beginTransaction();
        Map<String, Object> params = new HashMap<>();
        params.put("props", map.values());
        tx.run("UNWIND $props AS map MERGE (a:Article {_id: map._id}) SET a.title = map.title, a.year = map.year", params);
        tx.commit();
    }

    public static String sanitize(String input) {
        return input.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("'", "\\'");
    }
}
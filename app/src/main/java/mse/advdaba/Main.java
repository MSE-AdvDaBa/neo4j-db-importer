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

        Driver driver = GraphDatabase.driver("bolt://" + neo4jIP + ":80", AuthTokens.basic("neo4j", "testtest"));
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
            readFileForEntities(jsonPath, session);
            readFileForCitations(jsonPath, session);
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

    public static void readFileForEntities(String jsonPath, Session session) {
        Map<String, Object> mapOfArticles = null;
        Map<String, Object> mapOfAuthors = null;
        JsonReader reader = null;
        try {
            reader = new JsonReader(new FileReader(jsonPath));
            reader.beginArray();
            mapOfArticles = new HashMap<>();
            mapOfAuthors = new HashMap<>();
            int i = 0;
            System.out.println("Creating articles and authors");
            while (reader.hasNext()) {
                if (i >= MAX_NODES) {
                    insertArticles(mapOfArticles, session);
                    insertAuthors(mapOfAuthors, session);
                    mapOfArticles = new HashMap<>();
                    mapOfAuthors = new HashMap<>();
                    i = 0;
                } else {
                    reader.beginObject();
                    HashMap<String, Object> articleMap = new HashMap<>();
                    while (reader.hasNext()) {
                        String name = reader.nextName();
                        switch (name) {
                            case "_id":
                                articleMap.put("_id", reader.nextString());
                                break;
                            case "title":
                                articleMap.put("title", sanitize(reader.nextString()));
                                break;
                            case "year":
                                articleMap.put("year", reader.nextInt());
                                break;
                            case "authors":
                                reader.beginArray();
                                while (reader.hasNext()) {
                                    reader.beginObject();
                                    HashMap<String, Object> authorMap = new HashMap<>();
                                    while (reader.hasNext()) {
                                        String authorName = reader.nextName();
                                        switch (authorName) {
                                            case "_id":
                                                authorMap.put("_id", reader.nextString());
                                                break;
                                            case "name":
                                                authorMap.put("name", sanitize(reader.nextString()));
                                                break;
                                            default:
                                                reader.skipValue();
                                                break;
                                        }
                                    }
                                    if (authorMap.get("_id") == null) {
                                        reader.endObject();
                                        continue;
                                    }
                                    mapOfAuthors.put(authorMap.get("_id").toString(), authorMap);
                                    reader.endObject();
                                    i++;
                                }
                                reader.endArray();
                                break;
                            default:
                                reader.skipValue();
                                break;
                        }
                    }
                    mapOfArticles.put(articleMap.get("_id").toString(), articleMap);
                    reader.endObject();
                    i++;
                }
            }
            insertArticles(mapOfArticles, session);
            insertAuthors(mapOfAuthors, session);
            reader.endArray();
            reader.close();
        } catch (IOException e) {
            System.out.println("Failed to read file");
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("Finished creating articles");
    }

    public static void readFileForCitations(String jsonPath, Session session) {
        Map<String, ArrayList<String>> mapOfArticleAuthors = null;
        Map<String, ArrayList<String>> mapOfArticleReferences = null;
        JsonReader reader = null;
        try {
            reader = new JsonReader(new FileReader(jsonPath));
            reader.beginArray();
            mapOfArticleAuthors = new HashMap<>();
            mapOfArticleReferences = new HashMap<>();
            int i = 0;
            System.out.println("Creating citations");
            while (reader.hasNext()) {
                if (i >= MAX_NODES) {
                    insertCitations(mapOfArticleAuthors, mapOfArticleReferences, session);
                    mapOfArticleAuthors = new HashMap<>();
                    mapOfArticleReferences = new HashMap<>();
                    i = 0;
                } else {
                    reader.beginObject();
                    HashMap<String, Object> articleMap = new HashMap<>();
                    while (reader.hasNext()) {
                        String name = reader.nextName();
                        switch (name) {
                            case "_id":
                                articleMap.put("_id", reader.nextString());
                                break;
                            case "authors":
                                reader.beginArray();
                                while (reader.hasNext()) {
                                    reader.beginObject();
                                    ArrayList<String> authors = new ArrayList<>();
                                    while (reader.hasNext()) {
                                        String authorName = reader.nextName();
                                        if (authorName.equals("_id")) {
                                            authors.add(reader.nextString());
                                        } else {
                                            reader.skipValue();
                                        }
                                    }
                                    // remove null values from arraylist
                                    authors.removeAll(Collections.singleton(null));
                                    mapOfArticleAuthors.put(articleMap.get("_id").toString(), authors);
                                    reader.endObject();
                                    i++;
                                }
                                reader.endArray();
                                break;
                            case "references":
                                ArrayList<String> references = new ArrayList<>();
                                reader.beginArray();
                                while (reader.hasNext()) {
                                    String referenceId = reader.nextString();
                                    references.add(referenceId);
                                    i++;
                                }
                                reader.endArray();
                                mapOfArticleReferences.put(articleMap.get("_id").toString(), references);
                                break;
                            default:
                                reader.skipValue();
                                break;
                        }
                    }
                    reader.endObject();
                    i++;
                }
            }
            insertCitations(mapOfArticleAuthors, mapOfArticleReferences, session);
            reader.endArray();
            reader.close();
        } catch (IOException e) {
            System.out.println("Failed to read file");
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("Finished creating citations");
    }

    public static void insertArticles(Map<String, Object> map, Session session) {
        System.out.println("Inserting " + map.size() + " articles");
        Transaction tx = session.beginTransaction();
        Map<String, Object> params = new HashMap<>();
        params.put("props", map.values());
        tx.run("UNWIND $props AS map MERGE (a:Article {_id: map._id}) SET a.title = map.title, a.year = map.year", params);
        tx.commit();
    }

    public static void insertAuthors(Map<String, Object> map, Session session) {
        System.out.println("Inserting " + map.size() + " authors");
        Transaction tx = session.beginTransaction();
        Map<String, Object> params = new HashMap<>();
        params.put("props", map.values());
        tx.run("UNWIND $props AS map MERGE (a:Author {_id: map._id}) SET a.name = map.name", params);
        tx.commit();
    }

    public static void insertCitations(Map<String, ArrayList<String>> mapOfArticleAuthors, Map<String, ArrayList<String>> mapOfArticleReferences, Session session) {
        int amountOfCitations = mapOfArticleAuthors.size() + mapOfArticleReferences.size();
        System.out.println("Inserting " + amountOfCitations + " citations");
        Transaction tx = session.beginTransaction();
        for (Map.Entry<String, ArrayList<String>> entry : mapOfArticleAuthors.entrySet()) {
            Map<String, Object> params = new HashMap<>();
            params.put("id", entry.getKey());
            params.put("authors", entry.getValue());
            tx.run("MATCH (a:Article {_id: $id}) UNWIND $authors AS author MERGE (b:Author {_id: author}) CREATE (b)-[:AUTHORED]->(a)", params);
        }
        tx.commit();
        tx = session.beginTransaction();
        for (Map.Entry<String, ArrayList<String>> entry : mapOfArticleReferences.entrySet()) {
            Map<String, Object> params = new HashMap<>();
            params.put("id", entry.getKey());
            params.put("references", entry.getValue());
            tx.run("MATCH (a:Article {_id: $id}) UNWIND $references AS reference MERGE (b:Article {_id: reference}) CREATE (a)-[:CITES]->(b)", params);
        }
        tx.commit();
    }

    public static String sanitize(String input) {
        return input.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("'", "\\'");
    }
}
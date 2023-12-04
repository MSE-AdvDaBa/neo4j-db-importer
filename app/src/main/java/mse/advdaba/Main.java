package mse.advdaba;

import com.google.gson.stream.JsonReader;

import com.google.gson.stream.JsonToken;
import org.neo4j.driver.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Main {
    public final static int MAX_NODES = Integer.parseInt(System.getenv("MAX_NODES"));
    public final static Date START = new Date();

    public static String elapsed(Date start, Date end) {
        DateTimeUtils dtUtils = new DateTimeUtils();
        return dtUtils.getDifference(start, end);
    }

    public static void main(String[] args) {
        String jsonPath = System.getenv("JSON_URL");
        System.out.println("URL to JSON file is " + jsonPath);
        System.out.println("Number of articles to consider is " + MAX_NODES);
        String neo4jIP = System.getenv("NEO4J_IP");
        System.out.println("IP address of neo4j server is " + neo4jIP);
        String neo4jPort = System.getenv("NEO4J_PORT");
        System.out.println("Port of neo4j server is " + neo4jPort);

        Driver driver = GraphDatabase.driver("bolt://" + neo4jIP + ":" + neo4jPort, AuthTokens.basic("neo4j", "testtest"));
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
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Failed to delete nodes, shutting down");
            System.exit(2);
        }

        try (Session session = driver.session()) {
            readFileForEntities(jsonPath, session);
            Date startCitations = new Date();
            readFileForCitations(jsonPath, session);
            Date endCitations = new Date();
            driver.close();
            System.out.println("Driver closed successfully");
            System.out.println("Finished creating articles and authors in " + elapsed(START, startCitations));
            System.out.println("Finished creating citations in " + elapsed(startCitations, endCitations));
            System.out.println("Total time elapsed: " + elapsed(START, endCitations));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("Failed to read file, shutting down");
            System.exit(2);
        }
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
        Map<String, Object> mapOfArticles;
        Map<String, Object> mapOfAuthors;
        URL url;
        JsonReader reader;
        try {
            url = new URL(jsonPath);
            reader = new CustomJsonReader(new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8)));
            reader.setLenient(true);
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
                                articleMap.put("title", reader.nextString());
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
                                                authorMap.put("name", reader.nextString());
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
        ArrayList<String[]> listOfArticlesAuthors;
        ArrayList<String[]> listOfArticleReferences;
        URL url;
        JsonReader reader;
        try {
            url = new URL(jsonPath);
            reader = new CustomJsonReader(new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8)));
            reader.setLenient(true);
            reader.beginArray();
            listOfArticlesAuthors = new ArrayList<>();
            listOfArticleReferences = new ArrayList<>();
            int i = 0;
            System.out.println("Creating citations");
            while (reader.hasNext()) {
                if (i >= MAX_NODES) {
                    insertCitations(listOfArticlesAuthors, listOfArticleReferences, session);
                    listOfArticlesAuthors = new ArrayList<>();
                    listOfArticleReferences = new ArrayList<>();
                    i = 0;
                } else {
                    reader.beginObject();
                    String currentId = null;
                    while (reader.hasNext()) {
                        String name = reader.nextName();
                        switch (name) {
                            case "_id":
                                currentId = reader.nextString();
                                break;
                            case "authors":
                                reader.beginArray();
                                while (reader.hasNext()) {
                                    reader.beginObject();
                                    while (reader.hasNext()) {
                                        String authorName = reader.nextName();
                                        if (authorName.equals("_id")) {
                                            String authorId = reader.nextString();
                                            if (authorId == null) {
                                                reader.endObject();
                                                continue;
                                            }
                                            listOfArticlesAuthors.add(new String[]{currentId, authorId});
                                        } else {
                                            reader.skipValue();
                                        }
                                    }
                                    reader.endObject();
                                    i++;
                                }
                                reader.endArray();
                                break;
                            case "references":
                                reader.beginArray();
                                while (reader.hasNext()) {
                                    String referenceId = reader.nextString();
                                    listOfArticleReferences.add(new String[]{currentId, referenceId});
                                    i++;
                                }
                                reader.endArray();
                                break;
                            default:
                                reader.skipValue();
                                break;
                        }
                    }
                    reader.endObject();
                }
            }
            insertCitations(listOfArticlesAuthors, listOfArticleReferences, session);
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

    public static void insertCitations(ArrayList<String[]> listOfArticlesAuthors, ArrayList<String[]> listOfArticleReferences, Session session) {
        int amountOfCitations = listOfArticlesAuthors.size() + listOfArticleReferences.size();
        System.out.println("Inserting " + amountOfCitations + " citations");
        Transaction tx = session.beginTransaction();
        tx.run("UNWIND $props AS map MATCH (a:Article {_id: map[0]}), (b:Author {_id: map[1]}) MERGE (a)-[:AUTHORED]->(b)", Collections.singletonMap("props", listOfArticlesAuthors));
        tx.commit();
        tx = session.beginTransaction();
        tx.run("UNWIND $props AS map MATCH (a:Article {_id: map[0]}), (b:Article {_id: map[1]}) MERGE (a)-[:CITES]->(b)", Collections.singletonMap("props", listOfArticleReferences));
        tx.commit();
    }
}
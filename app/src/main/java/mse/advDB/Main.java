package mse.advDB;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.driver.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Main {
    public final static int MAX_NODES = Integer.parseInt(System.getenv("MAX_NODES"));

    public final static long START = System.currentTimeMillis();

    public static double elapsed() {
        return (double) (System.currentTimeMillis() - START) / 1000d;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String jsonPath = System.getenv("JSON_FILE");
        System.out.println("Path to JSON file is " + jsonPath);
        System.out.println("Number of articles to consider is " + MAX_NODES);
        String neo4jIP = System.getenv("NEO4J_IP");
        System.out.println("IP address of neo4j server is " + neo4jIP);

        Driver driver = GraphDatabase.driver("bolt://" + neo4jIP + ":7687", AuthTokens.basic("neo4j", "testtest"));
        boolean connected = false;
        do {
            try {
                System.out.println("Sleeping a bit waiting for the db");
                Thread.yield();
                Thread.sleep(1000); // let some time for the neo4j container to be up and running
                driver.verifyConnectivity();
                connected = true;
            } catch (Exception e) {
                System.out.println("Failed to connect to db, retrying");
            }
        } while (!connected);
        System.out.println("Connected to db");
        try (Session session = driver.session()) {
            Transaction tx = session.beginTransaction();
            tx.run("MATCH (n) DETACH DELETE n");
            tx.commit();
            tx = session.beginTransaction();
            tx.run("create constraint article_id IF NOT EXISTS for (a:Article) REQUIRE a._id is UNIQUE");
            tx.commit();
        }
        try (Session session = driver.session()) {
            readFile(jsonPath, session);
        } catch (Exception e) {
            e.printStackTrace();
        }

        driver.close();
        System.out.println("Driver closed successfully");
        System.out.println("Finished in " + elapsed() + " seconds");
    }

    public static void readFile(String jsonPath, Session session) throws IOException, InterruptedException {
        Stack<Integer> nodeCheck = new Stack<>();
        StringBuilder jsonNode = new StringBuilder();
        ObjectMapper mapper = new ObjectMapper();
        FileReader fr = new FileReader(jsonPath);
        BufferedReader br = new BufferedReader(fr);
        Map<String, List<String>> authors = new HashMap<>();
        int crtNode = 0;
        FileWriter articleFile = new FileWriter("./csv/articles.csv");
        articleFile.write("_id§title§cites" + "\n");
        while (crtNode < MAX_NODES) {
            String line = br.readLine();
            if (line == null) {
                break;
            }
            if (line.equals("[") || line.equals("]")) {
                // skip first and last line
                continue;
            }

            //check {
            // works for the big file, the example one is formatted differently
            if (line.startsWith("{ ")) {
                nodeCheck.push(1);
            }

            //check {
            // same here, works only with the big file
            if (line.startsWith("},") || line.equals("}")) {
                nodeCheck.pop();
            }

            //append line to final node
            jsonNode.append(line);

            if (nodeCheck.empty()) {
                //add current node to list of nodes to parse
                JsonNode node = mapper.readTree(jsonNode.toString());
                appendAuthor(node, authors);
                articleFile.write(getArticleCSV(node) + "\n");

                //reset jsonData
                jsonNode.setLength(0);
                crtNode++;

            }
        }

        System.out.println("took " + elapsed() + " seconds to create CSV for all articles");

        writeAuthors(authors, "./csv/authors.csv");

        System.out.println("took " + elapsed() + " seconds to create CSV for all authors");

        System.out.println("Inserting articles");
        session.run("""
                CALL apoc.periodic.iterate(
                    "LOAD CSV WITH HEADERS FROM 'file:///csv/articles.csv' as line FIELDTERMINATOR '§' return line",
                    "WITH line, apoc.convert.fromJsonList(line.cites) as cites CREATE(:Article {_id:line._id,title:line.title,cites:cites})",
                    {batchSize:50,parallel:false}
                )
                """);

        System.out.println("inserting authors");

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

    public static void writeAuthors(Map<String, List<String>> authors, String path) throws IOException {

        FileWriter writer = new FileWriter(path);
        writer.write("_id§name§authored" + "\n");
        for (String authorId : authors.keySet()
        ) {
            List<String> author = authors.get(authorId);
            String line = authorId + "§" + sanitize(author.get(0)) + "§";
            author.remove(0);
            Collector<CharSequence, ?, String> collector = Collectors.joining("','", "['", "']");
            line += author.stream().collect(collector);
            writer.write(line + "\n");
        }
    }


    /**
     * cache authors in a hasmap to insert everything in one go
     *
     * @param node
     * @param authorsMap
     */
    public static void appendAuthor(JsonNode node, Map<String, List<String>> authorsMap) {
        JsonNode authors = node.get("authors");
        String nodeId = node.get("_id").asText();
        if (authors != null && authors.isArray()) {
            for (JsonNode author : authors
            ) {
                String authorName = "unknown";
                String authorId = "0";
                if (author.get("name") != null) {
                    authorName = sanitize(author.get("name").asText());
                }
                if (author.get("_id") != null) {
                    authorId = sanitize(author.get("_id").asText());
                }
                if (authorId.equals("0") && !authorName.equals("unknown")) {
                    authorId = authorName;
                }
                if (authorsMap.containsKey(authorId)) {
                    List<String> articles = authorsMap.get(authorId);
                    articles.add(nodeId);
                    authorsMap.put(authorId, articles);
                } else {
                    List<String> articles = new ArrayList<>();
                    articles.add(authorName);
                    articles.add(nodeId);
                    authorsMap.put(authorId, articles);
                }
            }
        }
    }

    public static void batchWrite(List<JsonNode> nodes, Session session) throws JsonProcessingException {
        List<String> lines = new ArrayList<>();
        for (JsonNode node : nodes
        ) {
            lines.add(getArticleCSV(node));
        }

        String param;
        if (lines.size() > 1) {
            Collector<CharSequence, ?, String> collector = Collectors.joining(",", "[", "]");
            param = lines.stream().collect(collector);
        } else {
            param = "[" + lines.get(0) + "]";
        }

        String query =
                "With " + param + " as lines\n" +
                        """
                                unwind lines as line
                                create(a:Article {_id:line[0],title:line[1],cites:line[2]})
                                """;
        boolean success = false;
        int attempts = 0;
        while (!success) {
            try {
                session.run(query);
                success = true;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                attempts++;
                System.out.println("Failed insertion, trying again");
                if (attempts > 15) {
                    System.out.println("Too many retries, shutting down");
                    System.exit(2);
                }
            }
        }
    }

    public static String getArticleCSV(JsonNode node) {

        List<String> citations = new ArrayList<>();

        JsonNode quotes = node.get("references");
        if (quotes != null && quotes.isArray()) {
            for (JsonNode citation : quotes) {
                citations.add("\"" + sanitize(citation.asText()) + "\"");
            }
        }
        Collector<CharSequence, ?, String> collector = Collectors.joining(",", "[", "]");
        String cites = citations.stream().collect(collector);

        String nodeTitle = "unknown title";
        String nodeId = "def";
        if (node.get("_id") != null) {
            nodeId = node.get("_id").asText();
        } else {
            System.out.println("id null");
            System.out.println(node.toPrettyString());
            System.exit(2);
        }
        if (node.get("title") != null) {
            nodeTitle = sanitize(node.get("title").asText());
        }

        Collector<CharSequence, ?, String> collector2 = Collectors.joining("§", "", "");
        List<String> output = new ArrayList<>();
        output.add(nodeId);
        output.add(nodeTitle);
        output.add(cites);

        return output.stream().collect(collector2);
    }


    public static String sanitize(String input) {
        return input.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("'", "\\'");
    }


}

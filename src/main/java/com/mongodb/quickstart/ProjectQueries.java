package com.mongodb.quickstart;

import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;


import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.mongodb.client.model.Aggregates.*;
public class ProjectQueries {

    public static void scraper(){

    }

    public static void query3(MongoDatabase db){
        // Number of videos for each category
        MongoCollection<Document> videoColl = db.getCollection("video_recordings");
        System.out.println("Query 3:");
        AggregateIterable<Document> counts = videoColl.aggregate(Collections.singletonList(
                group("$category", Accumulators.sum("count", 1))));

        for (Document doc : counts) {
            System.out.println(doc.get("_id") + ": " + doc.get("count"));
        }
        System.out.println(" ");
    }


    public static void query4(MongoDatabase db){
    // List the number of videos for each video category where the inventory is non-zero.
        MongoCollection<Document> videoColl = db.getCollection("video_recordings");
        System.out.println("Query 4:");
        AggregateIterable<Document> nonZeroCounts = videoColl.aggregate(Arrays.asList(
                match(Filters.gt("stock_count", 0)),
                group("$category", Accumulators.sum("count", 1))));


        for (Document doc : nonZeroCounts) {
            System.out.println(doc.get("_id") + ": " + doc.get("count"));
        }
        System.out.println(" ");

    }

    public static void query5(MongoDatabase db){
        // For each actor, list the video categories.
        // use the $lookup command to join collections
        MongoCollection<Document> video_actors = db.getCollection("video_actors");
        MongoCollection<Document> video_recordings = db.getCollection("video_recordings");
        System.out.println("Query 5:");

        Bson lookupPipeline = lookup("video_recordings", "recording_id", "recording_id", "recording");
        Bson unwindPipeline = unwind("$recording");

        Bson groupPipeline = group("$name");

        AggregateIterable<Document> temp = video_actors.aggregate(Arrays.asList(
                lookupPipeline
        ));

        for(Document doc : temp){
            System.out.println(doc);
        }
    }

    public static void query5_2(MongoDatabase db){
        MongoCollection<Document> collection = db.getCollection("videoDB");
        Map<String, ArrayList<String>> actorWithCategories = new HashMap<>();

        for (Document recording : collection.find()) { // every recording
            String category = recording.get("category").toString();
            String[] actorsList = recording.get("actors").toString().split(",");

            for (String actor : actorsList) { // sort through each actor in current recording
                ArrayList<String> categories;

                if (!actorWithCategories.containsKey(actor)) { // actor doesn't already exist in the map
                    categories = new ArrayList<>();
                } else {
                    categories = actorWithCategories.get(actor);

                }
                if (!categories.contains(category)) { // If the category not already in the actor's list
                    categories.add(category);
                }
                actorWithCategories.put(actor, categories);
            }
        }

        for (Map.Entry<String, ArrayList<String>> actor : actorWithCategories.entrySet()) {
            System.out.println(actor.getKey() + ": " + actor.getValue().toString());
        }
        System.out.println(" ");
    }

    public static void query6(MongoDatabase db){
    //   Which actors have appeared in movies in different video categories?



    }

    public static void query7(MongoDatabase db){
        // Which actors have not appeared in a comedy?


    }

    public static void query8(MongoDatabase db){
        // Which actors have appeared in comedy and action adventure movies?
        MongoCollection<Document> video_actors = db.getCollection("video_actors");
        MongoCollection<Document> video_recordings = db.getCollection("video_recordings");
        System.out.println("Query 8:");
    }

    public static void main(String[] args) {

        // https://docs.oracle.com/javase/7/docs/api/java/util/logging/Level.html#SEVERE gets rid of logging console output
        Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.SEVERE);

        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase db = mongoClient.getDatabase("videos");
            MongoDatabase db_test = mongoClient.getDatabase("test");

//            query3(db);
//            query4(db);
            query5(db);
//            query5_2(db_test);
            query6(db);
            query7(db);
            query8(db);

        }
    }
}

package com.mongodb.quickstart;

import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.mongodb.client.model.Aggregates.*;
public class ProjectQueries {


    public static void query3(MongoDatabase db){
        // Number of videos for each category
        MongoCollection<Document> videoColl = db.getCollection("video_recordings");
        System.out.println("Query 3:");
        AggregateIterable<Document> counts = videoColl.aggregate(Collections.singletonList(
                group("$category", Accumulators.sum("count", 1))));

        for (Document d : counts) {
            System.out.println(d.get("_id") + ": " + d.get("count"));
        }
    }


    public static void main(String[] args) {

        // https://docs.oracle.com/javase/7/docs/api/java/util/logging/Level.html#SEVERE gets rid of logging console output
        Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.SEVERE);

        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase db = mongoClient.getDatabase("videos");
            query3(db);


        }
    }
}

package com.mongodb.quickstart;

import com.mongodb.client.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.descending;

public class Read {

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("3840_final_project");
            MongoCollection<Document> actorsCollection = sampleTrainingDB.getCollection("video_actors");

            // find one document with new Document
            Document student1 = actorsCollection.find(new Document("id", 0)).first();
            System.out.println("Student 1: " + student1.toJson());
        }
    }
}
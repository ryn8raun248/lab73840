package com.mongodb.quickstart;

import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.print.Doc;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Arrays.*;


import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class ProjectQueries {

    public static void scraper(MongoDatabase db) throws IOException {
        MongoCollection<Document> videoActor = db.getCollection("video_actors");
        Iterable<Document> thing = videoActor.find();
        File file = new File("C:\\Users\\rhodesk\\IdeaProjects\\lab73840\\src\\main\\java\\com\\mongodb\\quickstart\\updated_real.json");
        List<String> lines = Files.readAllLines(file.toPath());
        int realIndex = 0;
        for(Document doc: thing){

                ArrayList<Document> documents = new ArrayList<>();
                int index = (lines.get(realIndex).indexOf("categories"));
                String temp = lines.get(realIndex).substring(index+13, lines.get(realIndex).lastIndexOf('"'));
                String[] categories = temp.split(",");
                for(String category: categories) {
                    documents.add(new Document("Category", category));
                }
                doc.append("Categories", documents);
                db.getCollection("new_video_actors").insertOne(doc);
                realIndex++;
        }
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
        MongoCollection<Document> video_actors = db.getCollection("new_video_actors");
        System.out.println("Query 5:");

        Bson projectPipeline = project(fields(include("name", "Categories")));
        Bson groupPipeline = group("$name", Accumulators.addToSet("Categories", "$Categories"));
        Bson unwindPipeline = unwind("$Categories");

        AggregateIterable<Document> temp = video_actors.aggregate(Arrays.asList(
            projectPipeline, unwindPipeline, groupPipeline
        ));



        for(Document doc : temp){
            System.out.print(doc.get("_id") + ":");
            ArrayList<Document> docList = (ArrayList)doc.get("Categories");

            for(Document element: docList){
                System.out.print(" " + element.get("Category") + ",");
            }
            System.out.println();
        }
        System.out.println();
    }

    public static void query6(MongoDatabase db){
        //Which actors have appeared in movies in different video categories?
        MongoCollection<Document> video_actors = db.getCollection("new_video_actors");

        System.out.println("Query 6:");

        Bson projectPipeline = project(fields(include("name", "Categories")));
        Bson groupPipeline = group("$name", Accumulators.addToSet("Categories", "$Categories"),
                Accumulators.sum("number",1 ));
        Bson filterPipeline = match(Filters.gt("number", 1));



        AggregateIterable<Document> temp = video_actors.aggregate(Arrays.asList(
                projectPipeline, groupPipeline, filterPipeline
        ));


        for(Document doc : temp){
            System.out.print(doc.get("_id") + ": ");

            System.out.print("Number of categories, " + doc.get("number"));
            System.out.println();
        }
        System.out.println();



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
            MongoDatabase db_test = mongoClient.getDatabase("test2");

//            query3(db);
//            query4(db);
//            query5(db);
            query6(db);
//            query7(db);
//            query8(db);


        }
    }
}

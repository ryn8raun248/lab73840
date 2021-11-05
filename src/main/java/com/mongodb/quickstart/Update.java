package com.mongodb.quickstart;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

import java.util.ArrayList;
import java.util.Arrays;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

public class Update {

    public static void main(String[] args) {
        JsonWriterSettings prettyPrint = JsonWriterSettings.builder().indent(true).build();

        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase sampleTrainingDB = mongoClient.getDatabase("test");
            MongoDatabase sampleTrainingDB2 = mongoClient.getDatabase("test2");

            MongoCollection<Document> recordings = sampleTrainingDB.getCollection("videoDB");
            MongoCollection<Document> recordings2 = sampleTrainingDB2.getCollection("videoDB");


            for (Document recording : recordings.find()) { // for each recording
                String[] actorsList = recording.get("actors").toString().split(","); // get list of actors in each recording
                ArrayList<Document> docList = new ArrayList<>();

                for (String actor: actorsList){
                    // create new object in array in MongoDB
                    docList.add(new Document("ActorName", actor));
                }

                recording.append("actors_new", docList);
                //recordings.updateOne(recording);

                recordings2.insertOne(recording);

                //System.out.println(recording.values());


                // update one document
//            Bson filter = eq("student_id", 10420);
//            Bson updateOperation = set("comment", "You should learn MongoDB!");
//            UpdateResult updateResult = gradesCollection.updateOne(filter, updateOperation);
//            System.out.println("=> Updating the doc with {\"student_id\":10420}. Adding comment.");
//            System.out.println(gradesCollection.find(filter).first().toJson(prettyPrint));
//            System.out.println(updateResult);
            }
        }
    }
}

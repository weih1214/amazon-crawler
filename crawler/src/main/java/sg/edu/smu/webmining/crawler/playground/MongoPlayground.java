package sg.edu.smu.webmining.crawler.playground;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.bson.Document;

/**
 * Created by hwei on 22/12/2016.
 */
public class MongoPlayground {
  public static void main(String[] args) {
    final MongoClient sourceMongoClient = new MongoClient("localhost", 27017);
    final MongoDatabase sourceMongoDatabase = sourceMongoClient.getDatabase("ProductPage");
    final MongoCollection<Document> sourceMongoCollection = sourceMongoDatabase.getCollection("content");

    FindIterable<Document> source = sourceMongoCollection.find().projection(Projections.include("Review Link"));

    for (Document doc : source) {
      final String temp = doc.get("Review Link").toString();
      if (temp.isEmpty()) {
        continue;
      }
      System.out.println(doc.get("Review Link"));
    }

  }
}

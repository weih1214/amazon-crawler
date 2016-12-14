package sg.edu.smu.webmining.crawler.pipeline;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by hwei on 12/12/2016.
 */
public class QuestionPipeline implements Pipeline {

  @Override
  public void process(ResultItems resultItems, Task task) {
    MongoClient mongoClient = new MongoClient("localhost", 27017);

    // Now connect to your databases and switch to the Collection
    MongoDatabase db = mongoClient.getDatabase("QuestionPage");
    MongoCollection<Document> collection = db.getCollection("content");
    org.bson.Document dbDoc = new org.bson.Document(resultItems.getAll());
    collection.insertOne(dbDoc);
    mongoClient.close();
  }
}

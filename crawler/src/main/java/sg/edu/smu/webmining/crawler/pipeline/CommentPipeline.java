package sg.edu.smu.webmining.crawler.pipeline;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

import java.util.Map;

/**
 * Created by hwei on 1/12/2016.
 */
public class CommentPipeline implements Pipeline {

    public void commentStorage(Map<String,Object> resultMap) {

        // To connect to MongoDB server
        @SuppressWarnings("resource")
        MongoClient mongoClient = new MongoClient("localhost", 27017);

        // Now connect to your databases and switch to the Collection
        @SuppressWarnings({})
        MongoDatabase db = mongoClient.getDatabase("CommentPage");
        MongoCollection<Document> collection = db.getCollection("content");

        org.bson.Document dbDoc = new org.bson.Document(resultMap);
        collection.insertOne(dbDoc);

        mongoClient.close();

    }

    @Override
    public void process(ResultItems resultItems, Task task) {
        for (String key: resultItems.getAll().keySet()) {
            commentStorage(resultItems.get(key));
        }
    }
}

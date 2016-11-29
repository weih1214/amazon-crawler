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
 * Created by hwei on 28/11/2016.
 */
public class ReviewPipeline implements Pipeline {

    public void reviewStorage(Map<String,Object> resultMap) {

        // To connect to MongoDB server
        @SuppressWarnings("resource")
        MongoClient mongoClient = new MongoClient("localhost", 27017);

        // Now connect to your databases and switch to the Collection
        @SuppressWarnings({})
        MongoDatabase db = mongoClient.getDatabase("ReviewPage");
        MongoCollection<Document> collection = db.getCollection("content");

        org.bson.Document dbDoc = new org.bson.Document(resultMap);
        collection.insertOne(dbDoc);

        mongoClient.close();

        }


    @Override
    public void process(ResultItems resultItems, Task task) {
        String s1 = resultItems.getRequest().getUrl().toString();
        if (s1.contains("/product-reviews/")) {
            for (String key: resultItems.getAll().keySet()) {
            reviewStorage(resultItems.get(key));
            }
        } else {
            reviewStorage(resultItems.getAll());
        }
    }
}

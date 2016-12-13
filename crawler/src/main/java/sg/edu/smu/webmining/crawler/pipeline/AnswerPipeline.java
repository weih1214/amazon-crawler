package sg.edu.smu.webmining.crawler.pipeline;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

/**
 * Created by hwei on 13/12/2016.
 */
public class AnswerPipeline implements Pipeline {

    @Override
    public void process(ResultItems resultItems, Task task) {
        // To connect to MongoDB server
        @SuppressWarnings("resource")
        MongoClient mongoClient = new MongoClient("localhost", 27017);

        // Now connect to your databases and switch to the Collection
        @SuppressWarnings({})
        MongoDatabase db = mongoClient.getDatabase("AnswerPage");
        MongoCollection<Document> collection = db.getCollection("content");
        for (String key: resultItems.getAll().keySet()) {
            Document answerDoc = new Document(resultItems.get(key));
            collection.insertOne(answerDoc);
        }
    }
}

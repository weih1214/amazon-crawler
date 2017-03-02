package sg.edu.smu.webmining.crawler.seedpagefetcher;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hwei on 16/1/2017.
 */
public class DBSeedpageManager implements SeedpageManager {

  private final MongoClient mongoClient;
  private final MongoDatabase mongoDatabase;
  private final MongoCollection<Document> mongoCollection;
  private final FindIterable<Document> queryResult;
  private final String fieldName;

  public DBSeedpageManager(String location, int port, String databaseName, String collectionName, String fieldName) {
    mongoClient = new MongoClient(location, port);
    mongoDatabase = mongoClient.getDatabase(databaseName);
    mongoCollection = mongoDatabase.getCollection(collectionName);
    queryResult = mongoCollection.find().projection(Projections.include(fieldName));
    this.fieldName = fieldName;
  }

  @Override
  public String[] get() {
    final List<String> seedpageList = new ArrayList<>();
    for (final Document doc : queryResult) {
      if (doc.get(fieldName) == null) {
        continue;
      }
      final String seedpage = doc.getString(fieldName);
      System.out.println(seedpage);
      seedpageList.add(seedpage);
    }
    mongoClient.close();
    return seedpageList.toArray(new String[seedpageList.size()]);
  }

  public String[] getImageList() {
    final List<String> imageList = new ArrayList<>();
    for (final Document doc : queryResult) {
      final List<String> imgs = (List<String>)doc.get(fieldName);
      if (imgs == null || imgs.size() == 0) {
        continue;
      }
      imageList.addAll(imgs);
    }
    mongoClient.close();
    return imageList.toArray(new String[imageList.size()]);
  }
}

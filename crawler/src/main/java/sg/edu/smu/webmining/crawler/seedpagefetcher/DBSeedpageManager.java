package sg.edu.smu.webmining.crawler.seedpagefetcher;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import us.codecraft.webmagic.Request;

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
  private final List<String> fieldNameList;

  public DBSeedpageManager(String location, int port, String databaseName, String collectionName, List<String> fieldNameList) {
    mongoClient = new MongoClient(location, port);
    mongoDatabase = mongoClient.getDatabase(databaseName);
    mongoCollection = mongoDatabase.getCollection(collectionName);
    queryResult = mongoCollection.find().projection(Projections.include(fieldNameList));
    this.fieldNameList = fieldNameList;
  }

  @Override
  public String[] get() {
    final List<String> seedpageList = new ArrayList<>();
    for (final Document doc : queryResult) {
      if (doc.get(fieldNameList.get(0)) == null) {
        continue;
      }
      final String seedpage = doc.getString(fieldNameList.get(0));
      System.out.println(seedpage);
      seedpageList.add(seedpage);
    }
    mongoClient.close();
    return seedpageList.toArray(new String[seedpageList.size()]);
  }

  public Request[] getRequestList() {
    final List<Request> requestList = new ArrayList <>();
    for (final Document doc : queryResult) {
      final List<String> imgs = (List<String>)doc.get(fieldNameList.get(0));
      final String id = doc.get(fieldNameList.get(1)).toString();
      if (imgs == null || imgs.size() == 0) {
        continue;
      }
      for (String s: imgs) {
        requestList.add(new Request(s).putExtra("ID", id));
      }
    }
    mongoClient.close();
    return requestList.toArray(new Request[requestList.size()]);
  }
/*
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
  */
}

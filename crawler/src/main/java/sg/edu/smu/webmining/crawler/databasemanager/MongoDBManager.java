package sg.edu.smu.webmining.crawler.databasemanager;

import java.util.Map;

/**
 * Created by hwei on 13/12/2016.
 */
public interface MongoDBManager {

    public void update(Map<String, Object> resultMap, String source) throws Exception;
}

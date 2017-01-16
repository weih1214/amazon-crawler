package sg.edu.smu.webmining.crawler.masterlist;

import java.util.List;

/**
 * Created by mtkachenko.2015 on 16/11/2016.
 */
public interface MasterListManager {

  public void update(String productId, String url, String source) throws Exception;

  public List<String> getAllUrls() throws Exception;

}

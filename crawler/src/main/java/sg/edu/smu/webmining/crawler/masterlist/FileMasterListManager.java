package sg.edu.smu.webmining.crawler.masterlist;

import java.io.*;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class FileMasterListManager implements MasterListManager, Closeable {

  private final PrintWriter out;

  public FileMasterListManager(String filename) throws FileNotFoundException, UnsupportedEncodingException {
    this(new File(filename));
  }

  public FileMasterListManager(File file) throws FileNotFoundException, UnsupportedEncodingException {
    out = new PrintWriter(file, "UTF-8");
  }

  @Override
  public void update(String productId, String url) throws IOException {
    out.write(productId);
    out.write('\t');
    out.write(url);
    out.println();
    out.flush();
  }


  @Override
  public List<String> getAllUrls() {
    return Collections.emptyList();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}

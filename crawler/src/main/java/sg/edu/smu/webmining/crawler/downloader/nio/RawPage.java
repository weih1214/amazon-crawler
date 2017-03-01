package sg.edu.smu.webmining.crawler.downloader.nio;

import us.codecraft.webmagic.Page;

/**
 * Created by hwei on 24/2/2017.
 */
public class RawPage extends Page {

  private byte[] raw;

  public RawPage() {
  }

  public byte[] getRawContent() {
    return raw;
  }

  public void setRawContent(byte[] raw) {
    this.raw = raw;
  }

}

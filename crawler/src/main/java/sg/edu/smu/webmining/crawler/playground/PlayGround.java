package sg.edu.smu.webmining.crawler.playground;

import org.apache.commons.collections.map.HashedMap;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by hwei on 13/12/2016.
 */
public class PlayGround {
    public static void main(String[] args) throws SQLException, IOException {
      Map<String, Object> map = new HashedMap();
      map.remove("source");

    }

    public static void changeList(int l1) {
      System.out.println(System.identityHashCode(l1));
      l1 = 5;
      System.out.println(System.identityHashCode(l1));
    }
}

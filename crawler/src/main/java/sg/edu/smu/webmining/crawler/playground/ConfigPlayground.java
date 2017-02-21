package sg.edu.smu.webmining.crawler.playground;


import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * Created by hwei on 23/1/2017.
 */
public class ConfigPlayground {
  public static void main(String[] args) throws FileNotFoundException {
//    Scanner sc = new Scanner(new File("D:\\config.json"));
//    StringBuilder sb = new StringBuilder();
//    while (sc.hasNext()) {
//      sb.append(sc.nextLine());
//    }
//    sc.close();
//    final String jsonStr = sb.toString();
//    JSONObject obj = new JSONObject(jsonStr);
//    Integer val = obj.getInt("Port");
//    System.out.println(val+5);
    FileInputStream file = new FileInputStream(new File("D:\\config.json"));
    JSONTokener jt = new JSONTokener(file);
    JSONObject obj = new JSONObject(jt);
    JSONObject obj2 = (JSONObject) obj.get("mongodb");
    System.out.println(obj2.get("port"));
    System.out.println(obj.optJSONArray("mongodb"));
//    System.out.println(obj.get("MysqlDatabaseName")+"\n"+obj.get("TableName")+"\n"+obj.get("Port")+"\n"+obj.get("FSLocation"));

  }
}

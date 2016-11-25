package sg.edu.smu.webmining.crawler.processor;

import org.jsoup.Jsoup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hwei on 25/11/2016.
 */
public class ManagerTesting {

    public static List<File> listFilesForFolder(final String folderPath) {
        File folder = new File(folderPath);
        List<File> files = new ArrayList<File>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry.getAbsolutePath());
            } else {
                files.add(fileEntry);
            }
        }
        return files;
    }

    public static List<org.jsoup.nodes.Document> load(List<File> files) {
        List<org.jsoup.nodes.Document> docList = new ArrayList<org.jsoup.nodes.Document>();
        for (File file : files) {
            try {
                org.jsoup.nodes.Document doc = Jsoup.parse(file, "UTF-8", "https://www.amazon.com/");
                docList.add(doc);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return docList;
    }
}

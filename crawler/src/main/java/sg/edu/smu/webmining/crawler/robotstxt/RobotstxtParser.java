/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sg.edu.smu.webmining.crawler.robotstxt;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.StringTokenizer;


/**
 * @author Yasser Ganjisaffar
 */
public class RobotstxtParser {

  private static final String PATTERNS_USERAGENT1 = "^User-agent: \\*";
  private static final String PATTERNS_USERAGENT2 = "User-agent:.*";
  private static final String PATTERNS_DISALLOW = "Disallow:.*";
  private static final String PATTERNS_ALLOW = "Allow:.*";

  //private static final int PATTERNS_USERAGENT_LENGTH = 12;
  private static final int PATTERNS_DISALLOW_LENGTH = 9;
  private static final int PATTERNS_ALLOW_LENGTH = 6;

  public static HostDirectives parse(String content) {

    HostDirectives directives = null;
    StringTokenizer st = new StringTokenizer(content, "\n\r");
    while (st.hasMoreTokens()) {
      String line = st.nextToken();

      // remove any html markup
      line = line.replaceAll("<[^>]+>", "");

      line = line.trim();

      if (line.isEmpty()) {
        continue;
      }

      if (line.matches(PATTERNS_USERAGENT1)) {
        continue;
      } else if (line.matches(PATTERNS_USERAGENT2)) {
        break;
      }

      if (line.matches(PATTERNS_DISALLOW)) {
        String path = line.substring(PATTERNS_DISALLOW_LENGTH).trim();
        if (path.endsWith("*")) {
          path = path.substring(0, path.length() - 1);
        }
        path = path.trim();
        if (path.length() != 0) {
          if (directives == null) {
            directives = new HostDirectives();
          }
          directives.addDisallow(path);
        }
      } else if (line.matches(PATTERNS_ALLOW)) {
        String path = line.substring(PATTERNS_ALLOW_LENGTH).trim();
        if (path.endsWith("*")) {
          path = path.substring(0, path.length() - 1);
        }
        path = path.trim();
        if (directives == null) {
          directives = new HostDirectives();
        }
        directives.addAllow(path);
      }
    }

    return directives;
  }

  @SuppressWarnings({"resource"})
  public static void main(String[] args) {

    String content = null;
    try {
      content = new Scanner(new File("D://robots.txt")).useDelimiter("\\Z").next();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    // System.out.println(content);

    HostDirectives Amazon = null;
    // RobotstxtParser temp = null;
    Amazon = parse(content);
    System.out.println(Amazon.disallows.size());
    System.out.println(Amazon.allows.size());
  }

}

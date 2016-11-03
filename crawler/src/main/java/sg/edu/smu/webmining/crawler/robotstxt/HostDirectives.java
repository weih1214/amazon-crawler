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

/**
 * @author Yasser Ganjisaffar
 */
public class HostDirectives {


  public final RuleSet disallows = new RuleSet();
  public final RuleSet allows = new RuleSet();
  private final long timeFetched;


  public HostDirectives() {
    timeFetched = System.currentTimeMillis();
  }


  public boolean Isallowed(String path) {
    return !disallows.containsPrefixOf(path) || allows.containsPrefixOf(path);
  }

  public void addDisallow(String path) {
    disallows.add(path);
  }

  public void addAllow(String path) {
    allows.add(path);
  }


}
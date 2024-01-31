/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.client.request;

public class RssOfferShuffleResultRequest {

  private String appId;
  private int shuffleId;
  private int mapIndex;
  private long taskAttemptId;

  public RssOfferShuffleResultRequest(
      String appId, int shuffleId, int mapIndex, long taskAttemptId) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.mapIndex = mapIndex;
    this.taskAttemptId = taskAttemptId;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getMapIndex() {
    return mapIndex;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }
}

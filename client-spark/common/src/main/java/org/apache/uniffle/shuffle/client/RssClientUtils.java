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

package org.apache.uniffle.shuffle.client;

import java.util.Map;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.util.BlockIdLayout;

public class RssClientUtils {
  public static Roaring64NavigableMap createBlockIdBitmap(
      int partitionId, Map<Long, Integer> blocks, BlockIdLayout blockIdLayout) {
    Roaring64NavigableMap bitmap = Roaring64NavigableMap.bitmapOf();
    blocks.forEach(
        (taskAttemptId, blockNum) -> {
          for (int sequenceNo = 0; sequenceNo < blockNum; sequenceNo++) {
            long blockId = blockIdLayout.getBlockId(sequenceNo, partitionId, taskAttemptId);
            bitmap.add(blockId);
          }
        });
    return bitmap;
  }
}

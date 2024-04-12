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
import java.util.stream.Collectors;

import scala.Console;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.common.util.BlockIdLayout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class RssClientUtilsTest {
  @Test
  public void createBlockIdBitmapTest() {
    BlockIdLayout layout = BlockIdLayout.DEFAULT;

    // no blocks
    Map<Long, Integer> blocks = Maps.newHashMap();
    long[] expecteds = new long[] {};
    Roaring64NavigableMap bitmap = RssClientUtils.createBlockIdBitmap(1, blocks, layout);
    assertArrayEquals(expecteds, bitmap.toArray());

    // zero blocks for one task attempt id
    blocks.put(2L, 0);
    expecteds = new long[] {};
    bitmap = RssClientUtils.createBlockIdBitmap(1, blocks, layout);
    assertArrayEquals(expecteds, bitmap.toArray());

    // one block for one task attempt id
    blocks.put(2L, 1);
    expecteds = new long[] {layout.getBlockId(0, 1, 2)};
    bitmap = RssClientUtils.createBlockIdBitmap(1, blocks, layout);
    assertArrayEquals(expecteds, bitmap.toArray());

    // twp blocks for one task attempt id
    blocks.put(2L, 2);
    expecteds = new long[] {layout.getBlockId(0, 1, 2), layout.getBlockId(1, 1, 2)};
    bitmap = RssClientUtils.createBlockIdBitmap(1, blocks, layout);
    assertArrayEquals(expecteds, bitmap.toArray());

    // twp blocks for one task attempt id, and one for another task attempt id
    blocks.put(3L, 1);
    expecteds =
        new long[] {
          layout.getBlockId(0, 1, 2), layout.getBlockId(0, 1, 3), layout.getBlockId(1, 1, 2)
        };
    bitmap = RssClientUtils.createBlockIdBitmap(1, blocks, layout);
    assertArrayEquals(expecteds, bitmap.toArray());

    // different partition id
    expecteds =
        new long[] {
          layout.getBlockId(0, 3, 2), layout.getBlockId(0, 3, 3), layout.getBlockId(1, 3, 2)
        };
    bitmap = RssClientUtils.createBlockIdBitmap(3, blocks, layout);
    final BlockIdLayout l = layout;
    Console.print(
        bitmap.stream().mapToObj(e -> l.asBlockId(e).toString()).collect(Collectors.joining(", ")));
    assertArrayEquals(expecteds, bitmap.toArray());

    // different layout
    layout = BlockIdLayout.from(20, 21, 22);
    expecteds =
        new long[] {
          layout.getBlockId(0, 1, 2), layout.getBlockId(0, 1, 3), layout.getBlockId(1, 1, 2)
        };
    bitmap = RssClientUtils.createBlockIdBitmap(1, blocks, layout);
    assertArrayEquals(expecteds, bitmap.toArray());
  }
}

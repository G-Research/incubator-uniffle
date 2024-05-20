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

package org.apache.uniffle.common.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/** Implementations are thread-safe. */
public interface BlockIdSet {
  BlockIdSet add(long blockId);

  default BlockIdSet addAll(BlockIdSet blockIds) {
    return addAll(blockIds.stream());
  }

  default BlockIdSet addAll(Stream<Long> blockIds) {
    synchronized (this) {
      blockIds.forEach(this::add);
    }
    return this;
  }

  default BlockIdSet addAll(LongStream blockIds) {
    synchronized (this) {
      blockIds.forEach(this::add);
    }
    return this;
  }

  BlockIdSet remove(long blockId);

  default BlockIdSet removeAll(BlockIdSet blockIds) {
    return removeAll(blockIds.stream());
  }

  default BlockIdSet removeAll(Stream<Long> blockIds) {
    synchronized (this) {
      blockIds.forEach(this::remove);
    }
    return this;
  }

  default BlockIdSet removeAll(LongStream blockIds) {
    synchronized (this) {
      blockIds.forEach(this::remove);
    }
    return this;
  }

  BlockIdSet retainAll(BlockIdSet blockIds);

  boolean contains(long blockId);

  boolean containsAll(BlockIdSet blockIds);

  int getIntCardinality();

  long getLongCardinality();

  boolean isEmpty();

  void forEach(LongConsumer func);

  LongStream stream();

  BlockIdSet copy();

  byte[] serialize() throws IOException;

  // create new empty instance using default implementation
  static BlockIdSet empty() {
    return new RoaringBitmapBlockIdSet();
  }

  // create new instance from given block ids using default implementation
  static BlockIdSet of(long... blockIds) {
    BlockIdSet set = empty();
    set.addAll(Arrays.stream(blockIds));
    return set;
  }
}

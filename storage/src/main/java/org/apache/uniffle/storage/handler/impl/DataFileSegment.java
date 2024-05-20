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

package org.apache.uniffle.storage.handler.impl;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.util.BlockId;

public class DataFileSegment extends FileSegment {

  private List<BufferSegment> bufferSegments;

  public DataFileSegment(String path, long offset, int length, List<BufferSegment> bufferSegments) {
    super(path, offset, length);
    this.bufferSegments = bufferSegments;
  }

  public List<BufferSegment> getBufferSegments() {
    return bufferSegments;
  }

  public Set<BlockId> getBlockIds() {
    Set<BlockId> blockIds = Sets.newHashSet();
    for (BufferSegment bs : bufferSegments) {
      blockIds.add(bs.getBlockId());
    }
    return blockIds;
  }
}

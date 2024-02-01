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

package org.apache.uniffle.server;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.proto.RssProtos;

public class MockedShuffleServerGrpcService extends ShuffleServerGrpcService {

  private static final Logger LOG = LoggerFactory.getLogger(MockedShuffleServerGrpcService.class);

  // appId -> shuffleId -> partitionRequestNum
  private Map<String, Map<Integer, AtomicInteger>> appToPartitionRequest =
      JavaUtils.newConcurrentMap();

  private long mockedTimeout = -1L;

  private boolean mockSendDataFailed = false;

  private boolean recordGetShuffleResult = false;

  private long numOfFailedReadRequest = 0;
  private AtomicInteger failedReadRequest = new AtomicInteger(0);

  public void enableMockedTimeout(long timeout) {
    mockedTimeout = timeout;
  }

  public void enableMockSendDataFailed(boolean mockSendDataFailed) {
    this.mockSendDataFailed = mockSendDataFailed;
  }

  public void enableRecordGetShuffleResult() {
    recordGetShuffleResult = true;
  }

  public void disableMockedTimeout() {
    mockedTimeout = -1;
  }

  public void enableFirstNReadRequestToFail(int n) {
    numOfFailedReadRequest = n;
  }

  public void resetFirstNReadRequestToFail() {
    numOfFailedReadRequest = 0;
    failedReadRequest.set(0);
  }

  public MockedShuffleServerGrpcService(ShuffleServer shuffleServer) {
    super(shuffleServer);
  }

  @Override
  public void sendShuffleData(
      RssProtos.SendShuffleDataRequest request,
      StreamObserver<RssProtos.SendShuffleDataResponse> responseObserver) {
    if (mockSendDataFailed) {
      LOG.info("Add a mocked sendData failed on sendShuffleData");
      throw new RuntimeException("This write request is failed as mocked failure！");
    }
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on sendShuffleData");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    super.sendShuffleData(request, responseObserver);
  }

  @Override
  public void reportShuffleResult(
      RssProtos.ReportShuffleResultRequest request,
      StreamObserver<RssProtos.ReportShuffleResultResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on reportShuffleResult");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    super.reportShuffleResult(request, responseObserver);
  }

  @Override
  public void offerShuffleResult(
      RssProtos.OfferShuffleResultRequest request,
      StreamObserver<RssProtos.OfferShuffleResultResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on offerShuffleResult");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    super.offerShuffleResult(request, responseObserver);
  }

  @Override
  public void commitShuffleResult(
          RssProtos.CommitShuffleResultRequest request,
          StreamObserver<RssProtos.CommitShuffleResultResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on commitShuffleResult");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    super.commitShuffleResult(request, responseObserver);
  }

  @Override
  public void getShuffleResult(
      RssProtos.GetShuffleResultRequest request,
      StreamObserver<RssProtos.GetShuffleResultResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on getShuffleResult");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    if (numOfFailedReadRequest > 0) {
      int currentFailedReadRequest = failedReadRequest.getAndIncrement();
      if (currentFailedReadRequest < numOfFailedReadRequest) {
        LOG.info(
            "This request is failed as mocked failure, current/firstN: {}/{}",
            currentFailedReadRequest,
            numOfFailedReadRequest);
        throw new RuntimeException("This request is failed as mocked failure");
      }
    }
    super.getShuffleResult(request, responseObserver);
  }

  @Override
  public void getShuffleResultForMultiPart(
      RssProtos.GetShuffleResultForMultiPartRequest request,
      StreamObserver<RssProtos.GetShuffleResultForMultiPartResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on getShuffleResult");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    if (numOfFailedReadRequest > 0) {
      int currentFailedReadRequest = failedReadRequest.getAndIncrement();
      if (currentFailedReadRequest < numOfFailedReadRequest) {
        LOG.info(
            "This request is failed as mocked failure, current/firstN: {}/{}",
            currentFailedReadRequest,
            numOfFailedReadRequest);
        throw new RuntimeException("This request is failed as mocked failure");
      }
    }
    if (recordGetShuffleResult) {
      List<Integer> requestPartitions = request.getPartitionsList();
      Map<Integer, AtomicInteger> shuffleIdToPartitionRequestNum =
          appToPartitionRequest.computeIfAbsent(
              request.getAppId(), x -> JavaUtils.newConcurrentMap());
      AtomicInteger partitionRequestNum =
          shuffleIdToPartitionRequestNum.computeIfAbsent(
              request.getShuffleId(), x -> new AtomicInteger(0));
      partitionRequestNum.addAndGet(requestPartitions.size());
    }
    super.getShuffleResultForMultiPart(request, responseObserver);
  }

  @Override
  public void getShuffleTaskAttemptIds(
      RssProtos.GetShuffleTaskAttemptIdsRequest request,
      StreamObserver<RssProtos.GetShuffleTaskAttemptIdsResponse> responseObserver) {
    if (mockedTimeout > 0) {
      LOG.info("Add a mocked timeout on getShuffleTaskAttemptIds");
      Uninterruptibles.sleepUninterruptibly(mockedTimeout, TimeUnit.MILLISECONDS);
    }
    if (numOfFailedReadRequest > 0) {
      int currentFailedReadRequest = failedReadRequest.getAndIncrement();
      if (currentFailedReadRequest < numOfFailedReadRequest) {
        LOG.info(
                "This request is failed as mocked failure, current/firstN: {}/{}",
                currentFailedReadRequest,
                numOfFailedReadRequest);
        throw new RuntimeException("This request is failed as mocked failure");
      }
    }
    super.getShuffleTaskAttemptIds(request, responseObserver);
  }

  public Map<String, Map<Integer, AtomicInteger>> getShuffleIdToPartitionRequest() {
    return appToPartitionRequest;
  }

  @Override
  public void getMemoryShuffleData(
      RssProtos.GetMemoryShuffleDataRequest request,
      StreamObserver<RssProtos.GetMemoryShuffleDataResponse> responseObserver) {
    if (numOfFailedReadRequest > 0) {
      int currentFailedReadRequest = failedReadRequest.getAndIncrement();
      if (currentFailedReadRequest < numOfFailedReadRequest) {
        LOG.info(
            "This request is failed as mocked failure, current/firstN: {}/{}",
            currentFailedReadRequest,
            numOfFailedReadRequest);
        throw new RuntimeException("This request is failed as mocked failure");
      }
    }
    super.getMemoryShuffleData(request, responseObserver);
  }
}

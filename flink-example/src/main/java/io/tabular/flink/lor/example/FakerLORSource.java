/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.iceberg.flink.lor.example;

import com.github.javafaker.Faker;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class FakerLORSource extends RichParallelSourceFunction<Map> {
  private volatile boolean cancelled = false;

  private float eventInterval;

  public void setEventInterval(float eventInterval) {
    this.eventInterval = eventInterval;
  }

  private final long fiveHundredYearsAgo = Instant.now().minus(Duration.ofDays(500 * 365)).getEpochSecond();

  private final long now = Instant.now().getEpochSecond();

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public void run(SourceContext<Map> ctx) throws Exception {
    Faker faker = new Faker();
    while (!cancelled) {
      long timeout = (long) eventInterval;
      TimeUnit.MILLISECONDS.sleep(timeout); // processing delay between each record
      synchronized (ctx.getCheckpointLock()) {
        ctx.collect(getRandomRecord(faker));
      }
    }
  }

  private Map<String, Object> getRandomRecord(Faker faker) {
    Map<String, Object> record = new HashMap<>();
    long random = ThreadLocalRandom
        .current()
        .nextLong(fiveHundredYearsAgo, now);
    record.put("character", faker.lordOfTheRings().character());
    record.put("location", faker.lordOfTheRings().location());
    record.put("event_time", Instant.ofEpochSecond(random));
    return record;
  }

  @Override
  public void cancel() {
    cancelled = true;
  }
}

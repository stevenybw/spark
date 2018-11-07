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

package org.apache.spark.shuffle.sort;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.MetadataShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import javax.annotation.Nullable;
import java.io.*;

final class CombinedShuffleWriter<K, V> extends ShuffleWriter<K, V> {
  private static final Logger logger = LoggerFactory.getLogger(CombinedShuffleWriter.class);
  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }
  }
  static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final Partitioner partitioner;
  private final SerializerInstance serializerInstance;
  private CombinedShuffleWriterBackend shuffleWriterBackend;
  private MyByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  CombinedShuffleWriter(
      BlockManager blockManager,
      MetadataShuffleBlockResolver shuffleBlockResolver,
      BypassMergeSortShuffleHandle<K, V> handle,
      int mapId,
      TaskContext taskContext,
      BufferedOutputStream[] bufferedOutputStreams,
      SparkConf conf) {
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.partitioner = dep.partitioner();
    this.serializerInstance = dep.serializer().newInstance();
    int numPartition = partitioner.numPartitions();
    this.shuffleWriterBackend = new CombinedShuffleWriterBackend(numPartition, bufferedOutputStreams);

    this.serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    this.serOutputStream = serializerInstance.serializeStream(serBuffer);
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    if (!records.hasNext()) {
      return;
    }
    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      final K key = record._1();
      int partitionId = partitioner.getPartition(key);
      serBuffer.reset();
      serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
      serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
      serOutputStream.flush();
      final int serializedRecordSize = serBuffer.size();
      assert(serializedRecordSize > 0);
      shuffleWriterBackend.insertRecord(serBuffer.getBuf(), serializedRecordSize, partitionId);
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      return None$.empty();
    }
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gora.cassandra.store;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.util.Utf8;
import org.apache.gora.GoraTestDriver;
import org.apache.gora.cassandra.GoraCassandraTestDriver;
import org.apache.gora.examples.generated.SensorContext;
import org.apache.gora.examples.generated.SensorData;
import org.apache.gora.examples.generated.SensorKey;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for Gora Cassandra composite primary key feature
 *
 * @author c.zirpins
 */
public class TestGoraCassandraComplexKeys {
  final String host = "localhost:9160";
  final String cluster = "Gora Cassandra Test Cluster";
  final String keyspace = "Sensor";
  final int replicationFactor = 1;

  private boolean dataCreated;

  private Map<SensorKey, SensorData> readings = new HashMap<SensorKey, SensorData>();
  private List<SensorKey> orderedKeys = new ArrayList<SensorKey>();

  private SensorKey startKey;
  private SensorKey endKey;

  private static GoraTestDriver testDriver = new GoraCassandraTestDriver();
  private static DataStore<SensorKey, SensorData> sensorDataStore;

  @BeforeClass
  public static void setUpClass() throws Exception {
    testDriver.setUpClass();
  }

  @Before
  public void setUp() throws Exception {
    sensorDataStore = testDriver.createDataStore(SensorKey.class, SensorData.class);
    testDriver.setUp();
  }

  @After
  public void tearDown() throws Exception {
    testDriver.tearDown();;
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    testDriver.tearDownClass();;
  }

  @Test
  public void testCreate() {
    if (dataCreated)
      return;

    // write readings
    Date date = new Date();
    long datebase = date.getTime();

    for (int i = 0; i < 1000; i++) {
      Random rand = new Random();
      // sample reading
      SensorData reading = new SensorData();
      reading.setReading(rand.nextDouble());
      reading.getParams().put(new Utf8("itchy-" + i), new Utf8("foo-" + i));
      reading.getParams().put(new Utf8("scratchy-" + i), new Utf8("bar-" + i));
      SensorContext ctx = new SensorContext();
      ctx.setMem(rand.nextDouble());
      ctx.setPower(rand.nextDouble());
      reading.setContext(ctx);
      int ne = rand.nextInt(10) + 1;
      for (int s = 0; s < ne; s++)
        reading.getEvents().add(rand.nextInt(100));
      // sample key
      SensorKey key = new SensorKey();
      key.setSensorId(new Utf8("foo"));
      GregorianCalendar gc = new GregorianCalendar();
      gc.setTime(date);
      key.setDate(datebase++);
      key.setYear(gc.get(GregorianCalendar.YEAR));
      // keep for testing
      readings.put(key, reading);
      orderedKeys.add(key);
      // put in store
      sensorDataStore.put(key, reading);
      // save start/end keys for query test
      if (i == 0)
        startKey = key;
      if (i == 999)
        endKey = key;
    }
    sensorDataStore.flush();
    dataCreated = true;
  }

  @Test
  public void testSingleRead() {
    if (!dataCreated) {
      testCreate();
      dataCreated = true;
    }
    // SINGLE get
    for (SensorKey key : readings.keySet()) {
      SensorData read = sensorDataStore.get(key);
      Double oldR = readings.get(key).getReading();
      Double newR = read.getReading();
      assertEquals(newR, oldR, 0.001);
    }
  }

  @Test
  public void testColumnScan() throws IOException, Exception {
    if (!dataCreated) {
      testCreate();
      dataCreated = true;
    }
    // COLUMNSCAN
    Query<SensorKey, SensorData> query = sensorDataStore.newQuery();
    query.setStartKey(startKey);
    query.setEndKey(endKey);
    query.setLimit(1000);
    Result<SensorKey, SensorData> result = sensorDataStore.execute(query);

    // check correctness and completeness
    int count = 0;
    while (result.next()) {
      count++;
      Double oldR = readings.get(result.getKey()).getReading();
      SensorData nextRead = result.get();
      Double newR = nextRead.getReading();
      assertEquals(newR, oldR, 0.001);
    }
    assertEquals(count, 1000);

    // this one checks for order too
    result = sensorDataStore.execute(query);
    for (SensorKey key : orderedKeys) {
      result.next();
      Double oldR = readings.get(key).getReading();
      SensorData nextRead = result.get();
      Double newR = nextRead.getReading();
      assertEquals(newR, oldR, 0.001);
    }
  }

  @Test
  public void testBlankQuery() {
    if (!dataCreated) {
      testCreate();
      dataCreated = true;
    }
    Query<SensorKey, SensorData> query = sensorDataStore.newQuery();
    Result<SensorKey, SensorData> result = sensorDataStore.execute(query);
    assertNotNull(result);
  }

//  @Test
//  public void testSchema() {
//    if (!sensorDataStore.schemaExists())
//      sensorDataStore.createSchema();
//
//    Cluster c = HFactory.getOrCreateCluster(cluster, new CassandraHostConfigurator(host));
//    KeyspaceDefinition kd = c.describeKeyspace(keyspace);
//    assertNotNull(kd);
//
//    assertTrue(kd.getReplicationFactor() == replicationFactor);
//
//    for (ColumnFamilyDefinition def : kd.getCfDefs())
//      if (def.getName().equals("meterReading")) {
//        assertEquals(
//            def.getComparatorType().getTypeName(),
//            "DynamicCompositeType(b=>org.apache.cassandra.db.marshal.BytesType,A=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.AsciiType),B=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.BytesType),a=>org.apache.cassandra.db.marshal.AsciiType,L=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.LongType),l=>org.apache.cassandra.db.marshal.LongType,I=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.IntegerType),i=>org.apache.cassandra.db.marshal.IntegerType,U=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UUIDType),T=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimeUUIDType),u=>org.apache.cassandra.db.marshal.UUIDType,t=>org.apache.cassandra.db.marshal.TimeUUIDType,s=>org.apache.cassandra.db.marshal.UTF8Type,S=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type),X=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.LexicalUUIDType),x=>org.apache.cassandra.db.marshal.LexicalUUIDType)");
//        assertEquals(
//            def.getKeyValidationClass(),
//            "org.apache.cassandra.db.marshal.DynamicCompositeType(b=>org.apache.cassandra.db.marshal.BytesType,A=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.AsciiType),B=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.BytesType),a=>org.apache.cassandra.db.marshal.AsciiType,L=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.LongType),l=>org.apache.cassandra.db.marshal.LongType,I=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.IntegerType),i=>org.apache.cassandra.db.marshal.IntegerType,U=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UUIDType),T=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimeUUIDType),u=>org.apache.cassandra.db.marshal.UUIDType,t=>org.apache.cassandra.db.marshal.TimeUUIDType,s=>org.apache.cassandra.db.marshal.UTF8Type,S=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type),X=>org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.LexicalUUIDType),x=>org.apache.cassandra.db.marshal.LexicalUUIDType)");
//      }
//  }

}
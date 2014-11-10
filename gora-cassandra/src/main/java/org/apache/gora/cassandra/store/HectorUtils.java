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

import java.nio.ByteBuffer;
import java.util.Arrays;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.gora.persistency.Persistent;

/**
 * This class it not thread safe.
 * According to Hector's JavaDoc a Mutator isn't thread safe, too.
 * Take a look at {@CassandraClient} for safe usage.
 */
public class HectorUtils<K,T extends Persistent> {

  public static void insertColumn(Mutator<DynamicComposite> mutator, DynamicComposite key, 
      String columnFamily, DynamicComposite columnName, ByteBuffer columnValue, String ttlAttr) {
    mutator.insert(key, columnFamily, createColumn(columnName, columnValue, ttlAttr));
  }

  public static HColumn<DynamicComposite, ByteBuffer> createColumn(DynamicComposite name, ByteBuffer value, String ttlAttr) {
    int ttl = Integer.parseInt(ttlAttr);
    HColumn<DynamicComposite, ByteBuffer> col = HFactory.createColumn(name, value, DynamicCompositeSerializer.get(), ByteBufferSerializer.get());

    if( 0 < ttl ) {
      col.setTtl( ttl ); 
    }

    return col;
  }

  public static HColumn<ByteBuffer,ByteBuffer> createColumn(ByteBuffer name, ByteBuffer value, String ttlAttr) {
    int ttl = Integer.parseInt(ttlAttr);
    HColumn<ByteBuffer,ByteBuffer> col = HFactory.createColumn(name, value, ByteBufferSerializer.get(), ByteBufferSerializer.get());

    if( 0 < ttl ) {
      col.setTtl( ttl ); 
    }

    return col;
  }

  public static HColumn<String,ByteBuffer> createColumn(String name, ByteBuffer value, String ttlAttr) {
    int ttl = Integer.parseInt(ttlAttr);
    HColumn<String,ByteBuffer> col = HFactory.createColumn(name, value, StringSerializer.get(), ByteBufferSerializer.get());

    if( 0 < ttl ) {
      col.setTtl( ttl );
    }

    return col;
  }

  public static HColumn<Integer,ByteBuffer> createColumn(Integer name, ByteBuffer value, String ttlAttr) {
    int ttl = Integer.parseInt(ttlAttr);
    HColumn<Integer,ByteBuffer> col = HFactory.createColumn(name, value, IntegerSerializer.get(), ByteBufferSerializer.get());

    if( 0 < ttl ) {
      col.setTtl( ttl );
    }

    return col;
  }


  public static void insertSubColumn(Mutator<DynamicComposite> mutator, DynamicComposite key, 
      String columnFamily, DynamicComposite superColumnName,  ByteBuffer columnName, 
      ByteBuffer columnValue, String ttlAttr) {
    mutator.insert(key, columnFamily, createSuperColumn(superColumnName, columnName, columnValue, ttlAttr));
  }

  public static void insertSubColumn(Mutator<DynamicComposite> mutator, DynamicComposite key, 
      String columnFamily, DynamicComposite superColumnName, String columnName, 
      ByteBuffer columnValue, String ttlAttr) {
    mutator.insert(key, columnFamily, createSuperColumn(superColumnName, columnName, columnValue, ttlAttr));
  }

  public static void insertSubColumn(Mutator<DynamicComposite> mutator, DynamicComposite key, 
      String columnFamily, DynamicComposite superColumnName, Integer columnName, 
      ByteBuffer columnValue, String ttlAttr) {
    mutator.insert(key, columnFamily, createSuperColumn(superColumnName, columnName, columnValue, ttlAttr));
  }


  public static void deleteSubColumn(Mutator<DynamicComposite> mutator, DynamicComposite key, 
      String columnFamily, DynamicComposite superColumnName, ByteBuffer columnName) {
    mutator.subDelete(key, columnFamily, superColumnName, columnName, DynamicCompositeSerializer.get(), 
        ByteBufferSerializer.get());
  }

  public static void deleteColumn(Mutator<DynamicComposite> mutator, DynamicComposite key, 
      String columnFamily, ByteBuffer columnName){
    mutator.delete(key, columnFamily, columnName, ByteBufferSerializer.get());
  }

  public static<K> HSuperColumn<DynamicComposite,ByteBuffer,ByteBuffer> 
    createSuperColumn(DynamicComposite superColumnName, ByteBuffer columnName, ByteBuffer columnValue, 
      String ttlAttr) {
    return HFactory.createSuperColumn(superColumnName, Arrays.asList(
        createColumn(columnName, columnValue, ttlAttr)), DynamicCompositeSerializer.get(), 
        ByteBufferSerializer.get(), ByteBufferSerializer.get());
  }

  public static<K> HSuperColumn<DynamicComposite,String,ByteBuffer> createSuperColumn(DynamicComposite superColumnName, String columnName, ByteBuffer columnValue, String ttlAttr) {
    return HFactory.createSuperColumn(superColumnName, Arrays.asList(createColumn(columnName, columnValue, ttlAttr)), DynamicCompositeSerializer.get(), StringSerializer.get(), ByteBufferSerializer.get());
  }

  public static<K> HSuperColumn<DynamicComposite,Integer,ByteBuffer> createSuperColumn(DynamicComposite superColumnName, Integer columnName, ByteBuffer columnValue, String ttlAttr) {
    return HFactory.createSuperColumn(superColumnName, Arrays.asList(createColumn(columnName, columnValue, ttlAttr)), DynamicCompositeSerializer.get(), IntegerSerializer.get(), ByteBufferSerializer.get());
  }

}

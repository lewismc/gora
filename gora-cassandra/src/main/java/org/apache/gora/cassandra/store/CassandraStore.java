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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperSlice;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.gora.cassandra.query.CassandraQuery;
import org.apache.gora.cassandra.query.CassandraResult;
import org.apache.gora.cassandra.query.CassandraResultList;
import org.apache.gora.cassandra.query.CassandraSubColumn;
import org.apache.gora.cassandra.query.CassandraMixedRow;
import org.apache.gora.cassandra.query.CassandraSuperColumn;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.DirtyListWrapper;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.query.impl.PartitionQueryImpl;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.gora.cassandra.serializers.AvroSerializerUtil;
import org.apache.gora.util.GoraException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link org.apache.gora.cassandra.store.CassandraStore} is the primary class 
 * responsible for directing Gora CRUD operations into Cassandra. We (delegate) rely 
 * heavily on {@link org.apache.gora.cassandra.store.CassandraClient} for many operations
 * such as initialization, creating and deleting schemas (Cassandra Keyspaces), etc.
 * @param PK
 *           primary key class (not cassandra row key)
 * @param T
 *          persistent class
 */
public class CassandraStore<PK, T extends PersistentBase> extends DataStoreBase<PK, T> {

  /** Logging implementation */
  public static final Logger LOG = LoggerFactory.getLogger(CassandraStore.class);

  /** Consistency property level for Cassandra column families */
  private static final String COL_FAM_CL = "cf.consistency.level";

  /** Consistency property level for Cassandra read operations. */
  private static final String READ_OP_CL = "read.consistency.level";

  /** Consistency property level for Cassandra write operations. */
  private static final String WRITE_OP_CL = "write.consistency.level";

  /** Variables to hold different consistency levels defined by the properties. */
  public static String colFamConsLvl;
  public static String readOpConsLvl;
  public static String writeOpConsLvl;

  private CassandraClient<PK, T> cassandraClient = new CassandraClient<PK, T>();

  private CassandraKeyMapper<PK, T> keyMapper;

  /**
   * Fixed string with value "UnionIndex" used to generate an extra column based on 
   * the original field's name
   */
  public static String UNION_COL_SUFIX = "_UnionIndex";

  /**
   * Default schema index with value "0" used when AVRO Union data types are stored
   */
  public static int DEFAULT_UNION_SCHEMA = 0;

  /**
   * The values are Avro fields pending to be stored.
   *We want to iterate over the keys in insertion order. We don't want to lock the entire
   * collection before iterating over the keys, since in the meantime other threads are adding
   * entries to the map.
   */
  private Map<PK, T> buffer = Collections.synchronizedMap(new LinkedHashMap<PK, T>());

  public static final ThreadLocal<BinaryEncoder> encoders =
      new ThreadLocal<BinaryEncoder>();

  /**
   * Create a {@link java.util.concurrent.ConcurrentHashMap} for the 
   * datum readers and writers. 
   * This is necessary because they are not thread safe, at least not before 
   * Avro 1.4.0 (See AVRO-650).
   * When they are thread safe, it is possible to maintain a single reader and
   * writer pair for every schema, instead of one for every thread.
   * @see <a href="https://issues.apache.org/jira/browse/AVRO-650">AVRO-650</a>
   */
  public static final ConcurrentHashMap<String, SpecificDatumWriter<?>> writerMap = 
      new ConcurrentHashMap<String, SpecificDatumWriter<?>>();

  /**
   * Initialize is called when then the call to {@link
   * org.apache.gora.store.DataStoreFactory#createDataStore(Class<D> dataStoreClass, Class<K>
   * keyClass, Class<T> persistent, org.apache.hadoop.conf.Configuration conf)} is made. In this
   * case, we merely delegate the store initialization to the {@link
   * org.apache.gora.cassandra.store.CassandraClient#initialize(Class<K> keyClass, Class<T>
   * persistentClass)}.
   */
  public void initialize(Class<PK> primaryKeyClass, Class<T> persistentClass, Properties properties) {
    try {
      super.initialize(primaryKeyClass, persistentClass, properties);
      if (autoCreateSchema) {
        // If this is not set, then each Cassandra client should set its default
        // column family
        colFamConsLvl = DataStoreFactory.findProperty(properties, this, COL_FAM_CL, null);
        // operations
        readOpConsLvl = DataStoreFactory.findProperty(properties, this, READ_OP_CL, null);
        writeOpConsLvl = DataStoreFactory.findProperty(properties, this, WRITE_OP_CL, null);
      }
      this.cassandraClient.initialize(primaryKeyClass, persistentClass);
      this.keyMapper = cassandraClient.getKeyMapper();    
    } catch (Exception e) {
      LOG.error(e.getMessage());
      LOG.error(e.getStackTrace().toString());
    }
  }

  @Override
  public void close() {
    LOG.debug("close");
    flush();
  }

  @Override
  public void createSchema() {
    LOG.debug("creating Cassandra keyspace");
    this.cassandraClient.checkKeyspace();
  }

  @Override
  public boolean delete(PK key) {
    this.cassandraClient.deleteByKey(key);
    return true;
  }

  @Override
  public long deleteByQuery(Query<PK, T> query) {
    LOG.debug("delete by query " + query);
    return 0;
  }

  @Override
  public void deleteSchema() {
    LOG.debug("delete schema");
    this.cassandraClient.dropKeyspace();
  }

  /**
   *  When executing Gora Queries in Cassandra we query the Cassandra keyspace by families. When add
   * sub/supercolumns, Gora keys are mapped to Cassandra composite primary keys.
   */
  @Override
  public Result<PK, T> execute(Query<PK, T> query) {

    Map<String, List<String>> familyMap = this.cassandraClient.getFamilyMap(query);
    Map<String, String> reverseMap = this.cassandraClient.getReverseMap(query);

    CassandraQuery<PK, T> cassandraQuery = new CassandraQuery<PK, T>();
    cassandraQuery.setQuery(query);
    cassandraQuery.setFamilyMap(familyMap);

    CassandraResult<PK, T> cassandraResult = new CassandraResult<PK, T>(this, query);
    cassandraResult.setReverseMap(reverseMap);

    CassandraResultList<PK> cassandraResultList = new CassandraResultList<PK>();

    // We query Cassandra keyspace by families.
    for (String family : familyMap.keySet()) {
      if (family == null) {
        continue;
      }
      if (this.cassandraClient.isSuper(family)) {
        addSuperColumns(family, cassandraQuery, cassandraResultList);

      } else {
        addSubColumns(family, cassandraQuery, cassandraResultList);
      }
    }

    cassandraResult.setResultSet(cassandraResultList);

    return cassandraResult;
  }

  /**
   * When querying for columns, Gora keys are mapped to Cassandra Primary Keys consisting of
   * partition keys and column names. The Cassandra Primary Keys resulting from the query are mapped
   * back to Gora Keys. Each result row might contain clustered fields associated with multiple
   * persistent entities. Row columns are mapped to persistent entities by means of the clustering
   * information contained in their Gora key.
   */
  private void addSubColumns(String family, CassandraQuery<PK, T> cassandraQuery, CassandraResultList<PK> cassandraResultList) {
    // retrieve key range corresponding to the query parameters (triggers cassandra call)
    List<Row<DynamicComposite, DynamicComposite, ByteBuffer>> rows = this.cassandraClient.execute(cassandraQuery, family);

    // loop result rows
    for (Row<DynamicComposite, DynamicComposite, ByteBuffer> row : rows) {
      DynamicComposite compositeRowKey = row.getKey();
      ColumnSlice<DynamicComposite, ByteBuffer> columnSlice = row.getColumnSlice();

      // loop result columns
      for (HColumn<DynamicComposite, ByteBuffer> hColumn : columnSlice.getColumns()) {
        // extract complex primary key
        DynamicComposite compositeColumnName = hColumn.getName();
        PK partKey = cassandraClient.getKeyMapper().getPrimaryKey(compositeRowKey, compositeColumnName);

        // find associated slice in the result list
        CassandraMixedRow<PK> mixedRow = cassandraResultList.getMixedRow(partKey);
        if (mixedRow == null) {
          mixedRow = new CassandraMixedRow<PK>();
          cassandraResultList.putMixedRow(partKey, mixedRow);
          mixedRow.setKey(partKey);
        }

        // create column representation
        CassandraSubColumn<DynamicComposite> compositeColumn = new CassandraSubColumn<DynamicComposite>();
        compositeColumn.setValue(hColumn);
        compositeColumn.setFamily(family);

        // add column to slice
        mixedRow.add(compositeColumn);

      }// loop columns

    }// loop rows
  }

  /**
   * When querying for superColumns, Gora keys are mapped to Cassandra Primary Keys consisting of
   * partition keys and column names. The Cassandra Primary Keys resulting from the query are mapped
   * back to Gora Keys. Each result row might contain clustered fields associated with multiple
   * persistent entities. Row columns are mapped to persistent entities by means of the clustering
   * information contained in their Gora key.
   */
  private void addSuperColumns(String family, CassandraQuery<PK, T> cassandraQuery, 
      CassandraResultList<PK> cassandraResultList) {
    // retrieve key range corresponding to the query parameters (triggers cassandra call)
    List<SuperRow<DynamicComposite, DynamicComposite, ByteBuffer, ByteBuffer>> superRows = 
        this.cassandraClient.executeSuper(cassandraQuery, family);

    // loop result rows
    for (SuperRow<DynamicComposite, DynamicComposite, ByteBuffer, ByteBuffer> superRow : superRows) {
      DynamicComposite compositeSuperRowKey = superRow.getKey();
      SuperSlice<DynamicComposite, ByteBuffer, ByteBuffer> superSlice = superRow.getSuperSlice();

      // loop result columns    +      for (HSuperColumn<DynamicComposite, ByteBuffer, ByteBuffer> hSuperColumn : superSlice.getSuperColumns()) {
      // extract complex primary key
      DynamicComposite compositeSuperColumnName = hSuperColumn.getName();
      PK partKey = cassandraClient.getKeyMapper().getPrimaryKey(compositeSuperRowKey, 
          compositeSuperColumnName);

      // find associated slice in the result list
      CassandraMixedRow<PK> mixedRow = cassandraResultList.getMixedRow(partKey);
      if (mixedRow == null) {
        mixedRow = new CassandraMixedRow<PK>();
        cassandraResultList.putMixedRow(partKey, mixedRow);
        mixedRow.setKey(partKey);
      }

      // create column representation
      CassandraSuperColumn cassandraSuperColumn = new CassandraSuperColumn();
      cassandraSuperColumn.setValue(hSuperColumn);
      cassandraSuperColumn.setFamily(family);
      // add column to slice
      mixedRow.add(cassandraSuperColumn);

    }// loop superColumns

  }// loop superROws


  /**
   * Flush the buffer which is a synchronized {@link java.util.LinkedHashMap}
   * storing fields pending to be stored by 
   * {@link org.apache.gora.cassandra.store.CassandraStore#put(Object, PersistentBase)}
   * operations. Invoking this method therefore writes the buffered rows
   * into Cassandra.
   * @see org.apache.gora.store.DataStore#flush()
   */
  @Override
  public void flush() {

    Set<PK> keys = this.buffer.keySet();

    // this duplicates memory footprint
    @SuppressWarnings("unchecked")
    PK[] keyArray = (PK[]) keys.toArray();

    // iterating over the key set directly would throw 
    //ConcurrentModificationException with java.util.HashMap and subclasses
    for (PK key: keyArray) {
      T value = this.buffer.get(key);
      if (value == null) {
        LOG.info("Value to update is null for key: " + key);
        continue;
      }
      Schema schema = value.getSchema();

      for (Field field: schema.getFields()) {
        if (value.isDirty(field.pos())) {
          addOrUpdateField(key, field, field.schema(), value.get(field.pos()));
        }
      }
    }

    // remove flushed rows from the buffer as all 
    // added or updated fields should now have been written.
    for (PK key: keyArray) {
      this.buffer.remove(key);
    }
  }

  @Override
  public T get(PK key, String[] fields) {
    CassandraQuery<PK,T> query = new CassandraQuery<PK,T>();
    query.setDataStore(this);
    query.setKeyRange(key, key);

    if (fields == null){
      fields = this.getFields();
    }
    query.setFields(fields);

    query.setLimit(1);
    Result<PK,T> result = execute(query);
    boolean hasResult = false;
    try {
      hasResult = result.next();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return hasResult ? result.get() : null;
  }

  /**
   * @see org.apache.gora.store.DataStore#getPartitions(org.apache.gora.query.Query)
   *
   *      the cassandra interpretation of this method relates to the partition components of the
   *      primary key. Queries with ranges in any part of the complex partition key would possibly
   *      need to traverse multiple nodes and are generally not possible with non order-preserving
   *      partitioners.
   *
   *      The method will check for partition key ranges and try to decompose them into a set of key
   *      pairs with absolute partition keys (preserving possible cluster key ranges). Such a
   *      decomposition is generally only feasible if the size of the associated key set is not too
   *      big. Therefore, only a few key types are supported including integer, boolean and long
   *      types.
   *
   *      Furthermore, for multi-dimensional partition keys, the range semantics differs from the
   *      default cassandra behavior. In cassandra, composite keys are stored in lexical order. This
   *      means that ranges of higher-level keys include the complete value ranges of lower-level
   *      keys (possibly slightly reduced by ranges of lower-level keys). Because this would result
   *      in far too many absolute keys (and associated queries), only the specified lower-level
   *      keys (absolute value or range) are considered. This means that rows with a lower-level key
   *      outside the specified range are not part of the query result, whereas with default
   *      cassandra behavior they would be included.
   *
   *      As a consequence, the automatic partitioning function is practically restricted to cases,
   *      where the data model is explicitly designed for it. For instance this works quite well for
   *      queries that consider modestly wide ranges of single components within a composite
   *      partition key.
   *
   *      If there is no partition key range or decomposition of partition keys is not possible, a
   *      single partition query will be returned with a warning.
   *
   *      Locations are not really relevant from a cassandra point of view, because performance
   *      gains from local queries are not likely to be significant. As a possible TODO, the default
   *      node (and possibly also replica nodes) associated with a partition key could be computed.
   *
   */
  @Override
  public List<PartitionQuery<PK, T>> getPartitions(Query<PK, T> query)
      throws IOException {
    // result list
    List<PartitionQuery<PK, T>> partitions = new ArrayList<PartitionQuery<PK, T>>();

    // a map holding key pairs for distinct partitions
    Map<PK, PK> keyMap = keyMapper.decomposePartionKeys(query.getStartKey(), query.getEndKey());

    // create a partition query for each partition key pair.
    for (Entry<PK, PK> e : keyMap.entrySet()) {
      PartitionQueryImpl<PK, T> pqi = new PartitionQueryImpl<PK, T>(query, e.getKey(), e.getValue());
      pqi.setConf(getConf());
      partitions.add(pqi);
    } 
    return partitions;
  }

  /**
   * In Cassandra Schemas are referred to as Keyspaces
   * @return Keyspace
   */
  @Override
  public String getSchemaName() {
    return this.cassandraClient.getKeyspaceName();
  }

  @Override
  public Query<PK, T> newQuery() {
    Query<PK,T> query = new CassandraQuery<PK, T>(this);
    query.setFields(getFieldsToQuery(null));
    return query;
  }

  /**
   * 
   * When doing the 
   * {@link org.apache.gora.cassandra.store.CassandraStore#put(Object, PersistentBase)}
   * operation, the logic is as follows:
   * <ol>
   * <li>Obtain the Avro {@link org.apache.avro.Schema} for the object.</li>
   * <li>Create a new duplicate instance of the object (explained in more detail below) **.</li>
   * <li>Obtain a {@link java.util.List} of the {@link org.apache.avro.Schema} 
   * {@link org.apache.avro.Schema.Field}'s.</li>
   * <li>Iterate through the field {@link java.util.List}. This allows us to 
   * consequently process each item.</li>
   * <li>Check to see if the {@link org.apache.avro.Schema.Field} is NOT dirty. 
   * If this condition is true then we DO NOT process this field.</li>
   * <li>Obtain the element at the specified position in this list so we can 
   * directly operate on it.</li>
   * <li>Obtain the {@link org.apache.avro.Schema.Type} of the element obtained 
   * above and process it accordingly. N.B. For nested type ARRAY, MAP
   * RECORD or UNION, we shadow the checks in bullet point 5 above to infer that the 
   * {@link org.apache.avro.Schema.Field} is either at 
   * position 0 OR it is NOT dirty. If one of these conditions is true then we DO NOT
   * process this field. This is carried out in 
   * {@link org.apache.gora.cassandra.store.CassandraStore#getFieldValue(Schema, Type, Object)}</li>
   * <li>We then insert the Key and Object into the {@link java.util.LinkedHashMap} buffer 
   * before being flushed. This performs a structural modification of the map.</li>
   * </ol>
   * ** We create a duplicate instance of the object to be persisted and insert processed
   * objects into a synchronized {@link java.util.LinkedHashMap}. This allows 
   * us to keep all the objects in memory till flushing.
   * @see org.apache.gora.store.DataStore#put(java.lang.Object, 
   * org.apache.gora.persistency.Persistent).
   * @param key for the Avro Record (object).
   * @param value Record object to be persisted in Cassandra
   */
  @Override
  public void put(PK key, T value) {
    Schema schema = value.getSchema();
    @SuppressWarnings("unchecked")
    T p = (T) SpecificData.get().newRecord(value, schema);
    List<Field> fields = schema.getFields();
    for (int i = 1; i < fields.size(); i++) {
      if (!value.isDirty(i)) {
        continue;
      }
      // if field dirty
      Field field = fields.get(i);
      Type type = field.schema().getType();
      Object fieldValue = value.get(field.pos());
      Schema fieldSchema = field.schema();
      // check if field has a nested structure (array, map, record or union)
      fieldValue = getFieldValue(fieldSchema, type, fieldValue);
      p.put(field.pos(), fieldValue);
    }// loop fields
    // this performs a structural modification of the map
    this.buffer.put(key, p);
  }

  /**
   * For every field within an object, we pass in a field schema, Type and value.
   * This enables us to process fields (based on their characteristics) 
   * preparing them for persistence.
   * @param fieldSchema the associated field schema
   * @param type the field type
   * @param fieldValue the field value.
   * @return
   */
  private Object getFieldValue(Schema fieldSchema, Type type, Object fieldValue ){
    switch(type) {
    case RECORD:
      Persistent persistent = (Persistent) fieldValue;
      Persistent newRecord = (Persistent) SpecificData.get().newRecord(persistent, persistent.getSchema());
      for (Field member: fieldSchema.getFields()) {
        if (member.pos() == 0 || !persistent.isDirty()) {
          continue;
        }
        Schema memberSchema = member.schema();
        Type memberType = memberSchema.getType();
        Object memberValue = persistent.get(member.pos());
        newRecord.put(member.pos(), getFieldValue(memberSchema, memberType, memberValue));
      }
      fieldValue = newRecord;
      break;
    case MAP:
      Map<?, ?> map = (Map<?, ?>) fieldValue;
      fieldValue = map;
      break;
    case ARRAY:
      fieldValue = (List<?>) fieldValue;
      break;
    case UNION:
      // storing the union selected schema, the actual value will 
      // be stored as soon as we get break out.
      if (fieldValue != null){
        int schemaPos = getUnionSchema(fieldValue,fieldSchema);
        Schema unionSchema = fieldSchema.getTypes().get(schemaPos);
        Type unionType = unionSchema.getType();
        fieldValue = getFieldValue(unionSchema, unionType, fieldValue);
      }
      //p.put( schemaPos, p.getSchema().getField(field.name() + CassandraStore.UNION_COL_SUFIX));
      //p.put(fieldPos, fieldValue);
      break;
    default:
      break;
    }    
    return fieldValue;
  }

  /**
   * Add a field to Cassandra according to its type.
   * @param key     the key of the row where the field should be added
   * @param field   the Avro field representing a datum
   * @param schema  the schema belonging to the particular Avro field
   * @param value   the field value
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void addOrUpdateField(PK key, Field field, Schema schema, Object value) {
    Type type = schema.getType();
    // checking if the value to be updated is used for saving union schema
    if (field.name().indexOf(CassandraStore.UNION_COL_SUFIX) < 0){
      switch (type) {
      case STRING:
      case BOOLEAN:
      case INT:
      case LONG:
      case BYTES:
      case FLOAT:
      case DOUBLE:
      case FIXED:
        this.cassandraClient.addColumn(key, field.name(), value);
        break;
      case RECORD:
        if (value != null) {
          if (value instanceof PersistentBase) {
            PersistentBase persistentBase = (PersistentBase) value;            
            try {
              byte[] byteValue = AvroSerializerUtil.serializer(persistentBase, schema);
              this.cassandraClient.addColumn(key, field.name(), byteValue);
            } catch (IOException e) {
              LOG.warn(field.name() + " named record could not be serialized.");
            }
          } else {
            LOG.warn("Record with value: " + value.toString() + " not supported for field: " + field.name());
          }
        } else {
          LOG.warn("Setting content of: " + field.name() + " to null.");
          String familyName =  this.cassandraClient.getCassandraMapping().getFamily(field.name());
          this.cassandraClient.deleteColumn(key, familyName, this.cassandraClient.toByteBuffer(field.name()));
        }
        break;
      case MAP:
        if (value != null) {
          if (value instanceof Map<?, ?>) {            
            Map<CharSequence,Object> map = (Map<CharSequence,Object>)value;
            Schema valueSchema = schema.getValueType();
            Type valueType = valueSchema.getType();
            if (Type.UNION.equals(valueType)){
              Map<CharSequence,Object> valueMap = new HashMap<CharSequence, Object>();
              for (CharSequence mapKey: map.keySet()) {
                Object mapValue = map.get(mapKey);
                int valueUnionIndex = getUnionSchema(mapValue, valueSchema);
                valueMap.put((mapKey+UNION_COL_SUFIX), valueUnionIndex);
                valueMap.put(mapKey, mapValue);
              }
              map = valueMap;
            }

            String familyName = this.cassandraClient.getCassandraMapping().getFamily(field.name());

            // If map is not super column. We using Avro serializer. 
            if (!this.cassandraClient.isSuper( familyName )){
              try {
                byte[] byteValue = AvroSerializerUtil.serializer(map, schema);
                this.cassandraClient.addColumn(key, field.name(), byteValue);
              } catch (IOException e) {
                LOG.warn(field.name() + " named map could not be serialized.");
              }
            }else{
              this.cassandraClient.addStatefulHashMap(key, field.name(), map);              
            }
          } else {
            LOG.warn("Map with value: " + value.toString() + " not supported for field: " + field.name());
          }
        } else {
          // delete map
          LOG.warn("Setting content of: " + field.name() + " to null.");
          this.cassandraClient.deleteStatefulHashMap(key, field.name());
        }
        break;
      case ARRAY:
        if (value != null) {
          if (value instanceof DirtyListWrapper<?>) {
            DirtyListWrapper fieldValue = (DirtyListWrapper<?>)value;
            GenericArray valueArray = new Array(fieldValue.size(), schema);
            for (int i = 0; i < fieldValue.size(); i++) {
              valueArray.add(i, fieldValue.get(i));
            }
            this.cassandraClient.addGenericArray(key, field.name(), (GenericArray<?>)valueArray);
          } else {
            LOG.warn("Array with value: " + value.toString() + " not supported for field: " + field.name());
          }
        } else {
          LOG.warn("Setting content of: " + field.name() + " to null.");
          this.cassandraClient.deleteGenericArray(key, field.name());
        }
        break;
      case UNION:
        // adding union schema index
        String columnName = field.name() + UNION_COL_SUFIX;
        String familyName = this.cassandraClient.getCassandraMapping().getFamily(field.name());
        if(value != null) {
          int schemaPos = getUnionSchema(value, schema);
          LOG.debug("Union with value: " + value.toString() + " at index: " + schemaPos + " supported for field: " + field.name());
          this.cassandraClient.getCassandraMapping().addColumn(familyName, columnName, columnName);
          if (this.cassandraClient.isSuper( familyName )){
            this.cassandraClient.addSubColumn(key, columnName, columnName, schemaPos);
          }else{
            this.cassandraClient.addColumn(key, columnName, schemaPos);

          }
          //this.cassandraClient.getCassandraMapping().addColumn(familyName, columnName, columnName);
          // adding union value
          Schema unionSchema = schema.getTypes().get(schemaPos);
          addOrUpdateField(key, field, unionSchema, value);
          //this.cassandraClient.addColumn(key, field.name(), value);
        } else {
          LOG.warn("Setting content of: " + field.name() + " to null.");
          if (this.cassandraClient.isSuper( familyName )){
            this.cassandraClient.deleteSubColumn(key, field.name());
          } else {
            this.cassandraClient.deleteColumn(key, familyName, this.cassandraClient.toByteBuffer(field.name()));
          }
        }
        break;
      default:
        LOG.warn("Type: " + type.name() + " not considered for field: " + field.name() + ". Please report this to dev@gora.apache.org");
      }
    }
  }

  /**
   * Given an object and the object schema this function obtains,
   * from within the UNION schema, the position of the type used.
   * If no data type can be inferred then we return a default value
   * of position 0.
   * @param pValue
   * @param pUnionSchema
   * @return the unionSchemaPosition.
   */
  private int getUnionSchema(Object pValue, Schema pUnionSchema){
    int unionSchemaPos = 0;
    //    String valueType = pValue.getClass().getSimpleName();
    Iterator<Schema> it = pUnionSchema.getTypes().iterator();
    while ( it.hasNext() ){
      Type schemaType = it.next().getType();
      if (pValue instanceof CharSequence && schemaType.equals(Type.STRING))
        return unionSchemaPos;
      else if (pValue instanceof ByteBuffer && schemaType.equals(Type.BYTES))
        return unionSchemaPos;
      else if (pValue instanceof Integer && schemaType.equals(Type.INT))
        return unionSchemaPos;
      else if (pValue instanceof Long && schemaType.equals(Type.LONG))
        return unionSchemaPos;
      else if (pValue instanceof Double && schemaType.equals(Type.DOUBLE))
        return unionSchemaPos;
      else if (pValue instanceof Float && schemaType.equals(Type.FLOAT))
        return unionSchemaPos;
      else if (pValue instanceof Boolean && schemaType.equals(Type.BOOLEAN))
        return unionSchemaPos;
      else if (pValue instanceof Map && schemaType.equals(Type.MAP))
        return unionSchemaPos;
      else if (pValue instanceof List && schemaType.equals(Type.ARRAY))
        return unionSchemaPos;
      else if (pValue instanceof Persistent && schemaType.equals(Type.RECORD))
        return unionSchemaPos;
      unionSchemaPos ++;
    }
    // if we weren't able to determine which data type it is, then we return the default
    return DEFAULT_UNION_SCHEMA;
  }

  /**
   * Simple method to check if a Cassandra Keyspace exists.
   * @return true if a Keyspace exists.
   */
  @Override
  public boolean schemaExists() {
    LOG.info("schema exists");
    return cassandraClient.keyspaceExists();
  }

  public CassandraKeyMapper<PK, T> getKeyMapper() {
    return this.keyMapper;
  }
}

package org.apache.gora.cassandra.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.util.Utf8;
import org.apache.gora.cassandra.serializers.GoraSerializerTypeInferer;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.util.AvroUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Utility class transforming primary key data from/to cassandra dynamic composite row keys and
 * column names based on the mapping specification.
 *
 * @author c.zirpins
 *
 */
public class CassandraKeyMapper<PK, T extends PersistentBase> {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraClient.class);
  private CassandraMapping cassandraMapping;
  private Class<PK> primaryKeyClass;
  private Schema schema;

  public CassandraKeyMapper(Class<PK> keyClass, CassandraMapping mapping) {
    this.primaryKeyClass = keyClass;
    this.cassandraMapping = mapping;
    setSchema();
  }

  public CassandraKeyMapper(Class<PK> keyClass, Class<T> persistentClass) {
    this.primaryKeyClass = keyClass;
    this.cassandraMapping = CassandraMappingManager.getManager().get(persistentClass);
    setSchema();
  }

  private void setSchema() {
    if (!isPersistentPrimaryKey())
      schema = null;
    else
      schema = newKeyInstance().getSchema();
  }

  public Schema getSchema() {
    return schema;
  }

  /**
   * Get complex column name for member of complex field
   *
   * @param key
   * @param fieldName
   * @param memberQualifier
   * @return
   */
  public DynamicComposite getColumnName(PK key, String fieldName, Object memberQualifier, boolean isRangeDelimiter) {
    DynamicComposite dc = getColumnName(key, fieldName, isRangeDelimiter);
    dc.add(memberQualifier);
    return dc;
  }

  public DynamicComposite getColumnName(PK key, String fieldName, Integer memberQualifier, boolean isRangeDelimiter) {
    DynamicComposite dc = getColumnName(key, fieldName, isRangeDelimiter);
    dc.add(memberQualifier);
    return dc;
  }

  public DynamicComposite getColumnName(PK key, String fieldName, String memberQualifier, boolean isRangeDelimiter) {
    DynamicComposite dc = getColumnName(key, fieldName, isRangeDelimiter);
    dc.add(memberQualifier);
    return dc;
  }

  /**
   * Extract field name from composite column name
   *
   * @param columnName
   * @return
   */
  public String getFieldQualifier(DynamicComposite columnName) {
    // get number of clustering components in composite name
    int n = cassandraMapping.getColumnNameFieldsList().size();
    // field name follows directly behind
    Utf8 fieldName = columnName.get(n, Utf8Serializer.get());
    return fieldName.toString();
  }

  /**
   * Get complex column name for simple field
   *
   * @param key
   *          the primary key
   * @param fieldName
   *          qualifier identifying a persistent field
   * @param doFieldMapping
   *          switch to translate qualifier as defined in cassandra mapping
   * @return
   */
  public DynamicComposite getColumnName(PK key, String fieldName, boolean doFieldMapping) {
    DynamicComposite dc = null;
    if (fieldName == null)
      throw new IllegalArgumentException("Field qualifier must not be null.");

    // handle null keys which might come from a blank query
    if (key == null)
      return new DynamicComposite();

    // create clustering components
    if (key instanceof PersistentBase)
      dc = buildComposite((PersistentBase) key, cassandraMapping.getColumnNameFieldsList(), cassandraMapping.getColumnNameTypesMap());
    else
      dc = new DynamicComposite();

    // map field names to column name qualifiers (if requested)
    String colName = null;
    if (doFieldMapping)
      colName = this.cassandraMapping.getColumn(fieldName);
    else
      colName = fieldName;

    if (colName == null)
      throw new RuntimeException("Mapping error: field qualifier could not be mapped to a column name.");

    dc.addComponent(new Utf8(colName), Utf8Serializer.get(), ComparatorType.UTF8TYPE.getTypeName());

    return dc;
  }

  /**
   * Get complex row key for entity
   *
   * @param key
   * @return
   */
  public DynamicComposite getRowKey(PK key) {
    DynamicComposite dc = null;

    // handle null keys which might come from a blank query
    if (key == null)
      return new DynamicComposite();

    if (key instanceof PersistentBase) {
      dc = buildComposite((PersistentBase) key, cassandraMapping.getRowKeyFieldsList(), cassandraMapping.getRowKeyTypesMap());
    } else {
      dc = new DynamicComposite();
      dc.add(key);
    }
    return dc;
  }

  private DynamicComposite buildComposite(PersistentBase pKey, Collection<String> parts, Map<String, String> typeMap) {
    if (pKey == null || parts == null)
      throw new IllegalArgumentException();
    DynamicComposite dc = new DynamicComposite();
    Schema schema = pKey.getSchema();
    for (String part : parts) {
      Field field = schema.getField(part);
      if (field == null)
        throw new IllegalArgumentException("Key Mapping Error: Key field missing for part=" + part);
      int pos = schema.getField(part).pos();
      Type type = field.schema().getType();
      Object value = pKey.get(pos);
      if (value == null)
        throw new IllegalArgumentException("Key Mapping Error: value missing for part=" + part);
      dc.addComponent(value, GoraSerializerTypeInferer.getSerializer(type), typeMap.get(part));
    }
    return dc;
  }

  /**
   * Extracts primary key information from dynamic composite row keys and column names
   *
   * @param compositeRowKey
   * @param compositeColumnName
   * @return associated primary key
   */
  @SuppressWarnings("unchecked")
  public PK getPrimaryKey(DynamicComposite compositeRowKey, DynamicComposite compositeColumnName) {
    // handle simple primary keys with fixed mapping
    if (!isPersistentPrimaryKey()) {
      assert compositeRowKey.size() == 1;
      PK simplePrimaryKey = (PK) compositeRowKey.get(0, GoraSerializerTypeInferer.getSerializer(primaryKeyClass));
      return simplePrimaryKey;
    }
    // handle persistent primary key with custom mapping
    PersistentBase persistentPrimaryKey = newKeyInstance();
    updatePersistentPrimaryKey(compositeRowKey, persistentPrimaryKey, cassandraMapping.getRowKeyFieldsList());
    updatePersistentPrimaryKey(compositeColumnName, persistentPrimaryKey, cassandraMapping.getColumnNameFieldsList());
    return (PK) persistentPrimaryKey;
  }

  private void updatePersistentPrimaryKey(AbstractComposite composite, PersistentBase persistentPrimaryKey, List<String> fieldList) {
    Schema schema = persistentPrimaryKey.getSchema();
    int index = 0;
    for (String part : fieldList) {
      Field field = schema.getField(part);
      Type type = field.schema().getType();
      Object value = composite.get(index, GoraSerializerTypeInferer.getSerializer(type));
      persistentPrimaryKey.put(field.pos(), value);
      index++;
    }
  }

  protected boolean isPersistentPrimaryKey() {
    return PersistentBase.class.isAssignableFrom(primaryKeyClass);
  }

  private PersistentBase newKeyInstance() {
    PersistentBase primaryKey = null;
    try {
      primaryKey = (PersistentBase) primaryKeyClass.newInstance();
    } catch (Exception e) {
      LOG.error("Error creating primaryKey.", e);
    }
    return primaryKey;
  }

  /**
   * Checks if the column name part of the primary key defines a range
   *
   * TODO only checks for inequality of corresponding parts
   *
   * @param startKey
   * @param endKey
   * @return
   */
  public boolean isCassandraColumnScan(PK startKey, PK endKey) {
    if (startKey == null || endKey == null)
      return true;
    if (!isPersistentPrimaryKey())
      return false;
    else
      return !partsAreEqual((PersistentBase) startKey, (PersistentBase) endKey, cassandraMapping.getColumnNameFieldsList());
  }

  /**
   * Checks if the row key part of the primary key defines a range
   *
   * TODO only checks for inequality of corresponding parts
   *
   * @param startKey
   * @param endKey
   * @return
   */
  public boolean isCassandraRowScan(PK startKey, PK endKey) {
    if (startKey == null || endKey == null)
      return true;
    if (!isPersistentPrimaryKey())
      return !startKey.equals(endKey);
    else
      return !partsAreEqual((PersistentBase) startKey, (PersistentBase) endKey, cassandraMapping.getRowKeyFieldsList());
  }

  private boolean partsAreEqual(PersistentBase startKey, PersistentBase endKey, List<String> partList) {
    Schema schema = startKey.getSchema();
    boolean areEqual = true;
    for (String part : partList) {
      int pos = schema.getField(part).pos();
      areEqual &= startKey.get(pos).equals(endKey.get(pos));
    }
    return areEqual;
  }

  /**
   * Turns partition key ranges into sets of discrete partition keys.
   *
   * Cluster ranges will be preserved for each partition key pair.
   *
   * If decomposition is not possible, the original range will be returned. If the original keys do
   * not specify a range, they will be returned w/o changes.
   *
   * @param startKey
   * @param endKey
   * @return a map holding a set of start/end key pairs. Map key = query start key. Map value =
   *         query end key.
   */
  @SuppressWarnings("unchecked")
  public Map<PK, PK> decomposePartionKeys(PK startKey, PK endKey) {
    Map<PK, PK> result = new HashMap<PK, PK>();

    // if not a row scan return keys as they are
    if (!isCassandraRowScan(startKey, endKey)) {
      result.put(startKey, endKey);
      LOG.warn("The keys do not specify a partition key range. A single partition will be returned.");
      return result;
    }

    // case of simple keys
    if (!isPersistentPrimaryKey()) {
      // try decomposing simple key
      List<Object> keyRange = decomposeKeyRange(startKey, endKey);
      // if not possible return original range
      if (keyRange == null) {
        // no decomposition at all
        result.put(startKey, endKey);
        LOG.warn("Key decomposition not possible. Resulting key range will require an order-preserving partitioner.");
        return result;
      }
      for (Object o : keyRange) {
        result.put((PK) o, (PK) o);
      }
      return result;
    }

    // case of composite primary keys
    PersistentBase _startKey = (PersistentBase) startKey;
    PersistentBase _endKey = (PersistentBase) endKey;

    // 1 create decompose part ranges
    Map<String, List<Object>> partRanges = new HashMap<String, List<Object>>();

    // loop key parts
    for (String part : cassandraMapping.getRowKeyFieldsList()) {
      // compare key parts
      Field partField = schema.getField(part);
      Type partType = partField.schema().getType();
      int pos = partField.pos();
      boolean areEqual = _startKey.get(pos).equals(_endKey.get(pos));
      // if different, try to decompose
      List<Object> rangeList;
      if (!areEqual) {
        rangeList = decomposeKeyRange(partType, _startKey.get(pos), _endKey.get(pos));
        if (rangeList == null) {
          // no decomposition at all
          result.put(startKey, endKey);
          LOG.warn("Key decomposition not possible. Resulting key range will require an order-preserving partitioner.");
          return result;
        }
      } else {
        rangeList = Arrays.asList(_startKey.get(pos));
      }
      partRanges.put(part, rangeList);
    }

    // 2 recursively combine ranges
    // The key set is extended from lowest-level to highest-level partition key parts
    // Initially, the set contains the original range keys in order to preserve the cluster ranges
    result.put(startKey, endKey);
    List<String> reversedPartList = new ArrayList<String>(Lists.reverse(cassandraMapping.getRowKeyFieldsList()));
    result = (Map<PK, PK>) extendKeySet(partRanges, reversedPartList, (Map<PersistentBase, PersistentBase>) result);

    return result;
  }

  // recursive method to build a key set out of decomposed key parts
  private Map<PersistentBase, PersistentBase> extendKeySet(Map<String, List<Object>> partRanges, List<String> remainingParts,
      Map<PersistentBase, PersistentBase> lowerLevelKeys) {
    Map<PersistentBase, PersistentBase> thisLevelKeys = new HashMap<PersistentBase, PersistentBase>();
    String part = remainingParts.get(0);

    for (Entry<PersistentBase, PersistentBase> keyPair : lowerLevelKeys.entrySet()) {
      for (Object o : partRanges.get(part)) {
        PersistentBase subStartkey = (PersistentBase) AvroUtils.deepClonePersistent(keyPair.getKey());
        PersistentBase subEndkey = (PersistentBase) AvroUtils.deepClonePersistent(keyPair.getValue());
        int pos = getSchema().getField(part).pos();
        subStartkey.put(pos, o);
        subEndkey.put(pos, o);
        thisLevelKeys.put(subStartkey, subEndkey);
      }
    }

    // decommission old keypairs
    lowerLevelKeys.clear();

    // recursion break condition
    if (remainingParts.size() == 1)
      return thisLevelKeys;

    remainingParts.remove(part);
    return extendKeySet(partRanges, remainingParts, thisLevelKeys);
  }

  // decomposition of non-avro key range
  private List<Object> decomposeKeyRange(Object start, Object end) {
    Type partType = Type.NULL;
    if (start instanceof Integer)
      partType = Type.INT;
    else if (start instanceof Long)
      partType = Type.LONG;
    else if (start instanceof Boolean)
      partType = Type.BOOLEAN;
    else
      partType = Type.NULL;
    return decomposeKeyRange(partType, start, end);
  }

  // decomposition of key range for avro-typed parts
  private List<Object> decomposeKeyRange(Type partType, Object start, Object end) {
    List<Object> rangeList = new ArrayList<Object>();
    switch (partType) {
    case BOOLEAN:
      rangeList.add(new Boolean(true));
      rangeList.add(new Boolean(false));
      break;
    case INT:
      Integer intStart = (Integer) start;
      Integer intEnd = (Integer) end;
      for (int i = intStart; i <= intEnd; i++)
        rangeList.add(new Integer(i));
      break;
    case LONG:
      Long longStart = (Long) start;
      Long longEnd = (Long) end;
      for (long i = longStart; i <= longEnd; i++)
        rangeList.add(new Long(i));
      break;
    default:
      return null;
    }
    return rangeList;
  }

}

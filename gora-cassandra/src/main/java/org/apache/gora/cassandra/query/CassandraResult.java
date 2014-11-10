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

package org.apache.gora.cassandra.query;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.gora.cassandra.store.CassandraKeyMapper;
import org.apache.gora.cassandra.store.CassandraStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the result of a cassandra query
 *
 * @param PK
 *          Cassandra primary key base type
 * @param T
 *          persistent type
 */
public class CassandraResult<PK, T extends PersistentBase> extends ResultBase<PK, T> {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraResult.class);

  // pointer to current position in the result list
  private int resultListIndex;

  // the underlying result list holding slices (formerly named rows) of mixed columns
  private CassandraResultList<PK> cassandraResultList;

  // Maps Cassandra columns to Avro fields.
  private Map<String, String> reverseMap;

  private CassandraKeyMapper<PK, T> keyMapper;

  public CassandraResult(CassandraStore<PK, T> dataStore, Query<PK, T> query) {
    super(dataStore, query);
    this.keyMapper = dataStore.getKeyMapper();
  }

  @Override
  protected boolean nextInner() throws IOException {
    if (this.resultListIndex < this.cassandraResultList.size()) {
      updatePersistent();
    }
    ++this.resultListIndex;
    return (this.resultListIndex <= this.cassandraResultList.size());
  }

  /**
   * Gets the column containing the type of the union type element stored.
   * TODO: This might seem too much of an overhead if we consider that N slices have M columns, this
   * might have to be reviewed to get the specific column in O(1)
   * @param pFieldName
   * @param pColumns
   * @return
   */
  @SuppressWarnings("unused")
  private CassandraColumn<DynamicComposite> getUnionTypeColumn(String pFieldName, CassandraColumn<DynamicComposite>[] pColumns) {
    for (int iCnt = 0; iCnt < pColumns.length; iCnt++) {
      CassandraColumn<DynamicComposite> cColumn = pColumns[iCnt];
      String columnName = keyMapper.getFieldQualifier(cColumn.getName());
      if (pFieldName.equals(columnName))
        return cColumn;
    }
    return null;
  }


  /**
   * Load key/value pair from Cassandra row to Avro record.
   * @throws IOException
   */
  private void updatePersistent() throws IOException {
    CassandraMixedRow<PK> mixedResultRow = this.cassandraResultList.get(this.resultListIndex);

    // load key
    this.key = mixedResultRow.getKey();

    // load value
    Schema schema = this.persistent.getSchema();
    List<Field> fields = schema.getFields();

    for (CassandraColumn<DynamicComposite> cassandraColumn : mixedResultRow) {
      String family = cassandraColumn.getFamily();  
      DynamicComposite columnName = cassandraColumn.getName();
      String qualifier = keyMapper.getFieldQualifier(columnName);
      String fieldName = this.reverseMap.get(family + ":" + qualifier);

      // filter columns with respect to query fields
      // TODO look for some way to filter natively at query execution time
      if (fieldName == null) {
        continue;

      }
      // get field
      int pos = this.persistent.getSchema().getIndexNamed(fieldName);
      Field field = fields.get(pos);

      if (field == null) {
        LOG.debug("Field with name '" + fieldName + "' could not be matched to schema field.");
        return;
      }

      Type fieldType = field.schema().getType();

      if (fieldType == Type.UNION) {
        // TODO getting UNION stored type
        // TODO get value of UNION stored type. This field does not need to be written back to the
        // store
        cassandraColumn.setUnionType(getNonNullTypePos(field.schema().getTypes()));
      }

      // get value
      cassandraColumn.setField(field);
      Object value = cassandraColumn.getValue();

      // adjust value schema

      this.persistent.put(pos, value);
      // this field does not need to be written back to the store
      this.persistent.clearDirty(pos);
    }

  }

  //TODO Should we remove this method?
  //TODO review UNION handling
  private int getNonNullTypePos(List<Schema> pTypes){
    int iCnt = 0;
    for (Schema sch :  pTypes)
      if (!sch.getName().equals("null"))
        return iCnt;
      else 
        iCnt++;
    return CassandraStore.DEFAULT_UNION_SCHEMA;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public float getProgress() throws IOException {
    return (((float) this.resultListIndex) / this.cassandraResultList.size());
  }

  public void setResultSet(CassandraResultList<PK> cassandraResultList) {
    this.cassandraResultList = cassandraResultList;
  }

  public void setReverseMap(Map<String, String> reverseMap) {
    this.reverseMap = reverseMap;
  }

}

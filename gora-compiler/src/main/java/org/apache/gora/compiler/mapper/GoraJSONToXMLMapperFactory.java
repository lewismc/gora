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
package org.apache.gora.compiler.mapper;

import org.apache.gora.compiler.mapper.impl.GoraSolrJSONToXMLMapper;;

/**
 * The main factory implementation for initiating
 * appropriate JSON-to-XML mapping tools. Simple
 * flags can be used to invoke the correct mapping
 * implementation.
 */
public class GoraJSONToXMLMapperFactory {
  
  public static GoraJSONToXMLMapper getStoreMapper(String datastore) {
    GoraJSONToXMLMapper mapper = null;
    switch (datastore) {
    case "accumulo": 
      //mapper = new GoraAccumuloJSONToXMLMapper();
      break;
    case "cassandra":
      //mapper = new GoraCassandraJSONToXMLMapper();
      break;
    case "dynamodb":
      //mapper = new GoraDynamoDBJSONToXMLMapper();
      break;
    case  "hbase":
      //mapper = new GoraHBaseJSONToXMLMapper();
      break;
    case "mongodb":
      //mapper = new GoraMongoDBJSONToXMLMapper();
      break;
    case "solr":
      mapper = new GoraSolrJSONToXMLMapper();
      break;
    default:
      throw new RuntimeException("Mapping argument "+ datastore + " not recognized therefore mapping aborted!");
    }
    return mapper;
  }

}

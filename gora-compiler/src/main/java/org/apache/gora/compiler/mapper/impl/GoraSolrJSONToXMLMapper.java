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
package org.apache.gora.compiler.mapper.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.apache.avro.Schema;
import org.apache.camel.dataformat.xmljson.XmlJsonDataFormat;
import org.apache.gora.compiler.mapper.GoraJSONToXMLMapper;

/**
 * @author lmcgibbn
 *
 */
public class GoraSolrJSONToXMLMapper implements GoraJSONToXMLMapper {

  public void mapJSONToXML(File inputFile, File outputDir) throws IOException {
    try {
      executeMapping(inputFile, outputDir);
    } catch (XMLStreamException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.out.println("Mapping SUCCESSFUL: Mapped file: " + inputFile.getName() + 
        " to directory: " + outputDir.getAbsolutePath());
  }

  private void executeMapping(File file, File outputDir) throws IOException, XMLStreamException {
    System.out.println("Compiling: " + file.getAbsolutePath());
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(file);

    XmlJsonDataFormat namespacesFormat = new XmlJsonDataFormat();
    List<XmlJsonDataFormat.NamespacesPerElementMapping> namespaces = 
        new ArrayList<XmlJsonDataFormat.NamespacesPerElementMapping>();
    namespaces.add(new XmlJsonDataFormat.
        NamespacesPerElementMapping("", "|ns1|http://camel.apache.org/test1||http://camel.apache.org/default|"));
    namespaces.add(new XmlJsonDataFormat.
        NamespacesPerElementMapping("surname", "|ns2|http://camel.apache.org/personalData|" +
            "ns3|http://camel.apache.org/personalData2|"));
    namespacesFormat.setNamespaceMappings(namespaces);
    namespacesFormat.setRootName("?xml");

  }

}


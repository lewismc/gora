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
package org.apache.gora.compiler.mapper.cli;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.gora.compiler.mapper.GoraJSONToXMLMapper;
import org.apache.gora.compiler.mapper.GoraJSONToXMLMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple CLI for JSON-to-XML mapping tools
 * which uses <b>commons-cli</b> for printing
 * useful CLI to the terminal.
 */
public class GoraJSONXMLMappingCLI {

  private static final Logger LOG = LoggerFactory.getLogger(GoraJSONXMLMappingCLI.class);

  /**
   * @param args
   * @throws ParseException 
   */
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("d", "datastoreName", true, "String representation of the datastore mapping."
        + " This is required because XML mappings are datastore specific. Options include "
        + "accumulo, cassandra, dynamodb, hbase, mongodb and solr.");
    options.addOption("i", "inputJSON", true, "One or more fully qualifified paths "
        + "to a JSON file(s) for which we wish to auto-generate the "
        + "relevant gora-${datastore}-mapping.xml output.");
    options.addOption("o", "outputDir", true, "A fully qualified path to"
        + "an existing directory where we wish the auto-generated gora-${datastore}-mapping.xml "
        + "file(s) to be written to.");

    GnuParser parser = new GnuParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      if (cmd.getOptions().length != 3 && cmd.getArgs().length != 3) {
        printHelp(options);
      }
    } catch (ParseException pe) {
      throw new ParseException("REASON: Experienced error during execution of CLI parsing!");
    }

    //construct correct JSONToXMLMapper based on datastore optionValue
    GoraJSONToXMLMapper mapper = GoraJSONToXMLMapperFactory.getStoreMapper(cmd.getOptionValue("d").toLowerCase());

    //obtain the input file
    File inputJSON = new File(cmd.getOptionValue("i"));
    if(!inputJSON.isFile()) {
      printHelp(options);
      throw new IOException("REASON: Input file must be a JSON file.");
    }
    LOG.debug("Configured Mapper with input file: {}", inputJSON.getName());
    
    //obtain the output directory
    File outputDir = new File(cmd.getOptionValue("o"));
    if(!outputDir.isDirectory()) {
      printHelp(options);
      throw new IOException("REASON: Must supply a directory for the output XML.");
    }
    LOG.debug("Configured Mapper with output directory: {}", outputDir);

    //execute the JSONToXML mapping.
    try {
      mapper.mapJSONToXML(inputJSON, outputDir);
      LOG.info("JSON-to-XML mapping executed SUCCESSFULL.");
    } catch (Exception e) {
      printHelp(options);
      throw new RuntimeException("Error while reading input JSON schema file(s). "
          + "Check that the schemas are properly formatted.");
    }
  }

  private static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GoraJSONXMLMappingCLI.class.getSimpleName() + " <datastore> <input JSON path> [<input JSON path>...] <output XML path>", options);
  }
}

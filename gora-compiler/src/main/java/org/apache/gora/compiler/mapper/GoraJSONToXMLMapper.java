/**
 * 
 */
package org.apache.gora.compiler.mapper;

import java.io.File;
import java.io.IOException;

/**
 *
 */
public interface GoraJSONToXMLMapper {
  
  public void mapJSONToXML(File inputJSON, File outputDir) throws IOException;

}

package com.guidewire.tools.benchmarking;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.guidewire.cloudviewer.datamoving.CSVFileDefinitionJSON;
import org.apache.commons.csv.CSVRecord;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CSVFileDefinition contains the expected column definitions for a customer csv file upload. It validates the headers of the
 * submitted file and parses the data rows, ensuring that all required data is present and that it can be converted to the expected
 * data types.
 */
public class CSVFileDefinition {
  private String dataTableName;
  private List<CSVFileColumnDef> columnDefs;
  private transient List<String> unaliasedNamesInHeaderRowOrder;
  private QueryDefinition queryDefinition;
  private transient Map<String, CSVFileColumnDef> columnNameToColumnDefMap = new HashMap<>();

  public CSVFileDefinition(String dataTableName, List<CSVFileColumnDef> columnDefs) {
    this.dataTableName = dataTableName;
    setColumnDefs(dataTableName, columnDefs);
  }

  public CSVFileDefinition(String dataTableName) {
    this.dataTableName = dataTableName;
    this.columnDefs = new ArrayList<>();
    queryDefinition = new QueryDefinition();
    queryDefinition.setName(dataTableName);
  }

  // methods for validating and parsing data files
  /****************************************************************************************************************************************************/
  public boolean validateHeaderRow(Map<String, Integer> columnNames, DataExtractor extractor) {
    boolean noUnmatchedColumns = convertAliasesToNames(columnNames, extractor);
    boolean allRequiredColumns = validateAllRequiredColumnsArePresent(unaliasedNamesInHeaderRowOrder, extractor);
    return noUnmatchedColumns && allRequiredColumns;
  }

  public void parseRow( Map<String, String> columnNameToValueMap, CSVRecord row) {
    int i = 0;
    for (String columnName : unaliasedNamesInHeaderRowOrder) {
      columnNameToValueMap.put(columnName, row.get(i++));
    }
  }

  // getters and setters
  /****************************************************************************************************************************************************/
  public List<ColumnDef> getColumns() {
    return queryDefinition.getColumns();
  }

  public List<CSVFileColumnDef> getColumnDefs() {
    return columnDefs;
  }

  public QueryDefinition getQueryDefinition() {
    return queryDefinition;
  }

  public String getName() {
    return queryDefinition.getName();
  }

  public CSVFileColumnDef getColumnDefForColumnName(String columnName) {
    return columnNameToColumnDefMap.get(columnName);
  }

  public void addColumnDef(CSVFileColumnDef columnDef) {
    columnDefs.add(columnDef);
    columnNameToColumnDefMap.put(columnDef.getName(), columnDef);
    queryDefinition.addColumn(columnDef);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CSVFileDefinition)) {
      return false;
    }
    CSVFileDefinition fileDefinition = (CSVFileDefinition) obj;
    if (!dataTableName.equals(fileDefinition.dataTableName)) {
      return false;
    }
    if (columnDefs.size() != fileDefinition.columnDefs.size()) {
      return false;
    }
    for (CSVFileColumnDef column : fileDefinition.columnDefs) {
      if (!column.equals(columnNameToColumnDefMap.get(column.getName()))) {
        return false;
      }
    }
    return true;
  }

  /****************************************************************************************************************************************************/
  // methods for defining and updating the file definitions

  // Used by the DES. We don't send over omitted columns to the customer. If the customer includes an omitted column in the
  // data file it will cause an error because the column will not be in their file definition even though the column would be valid in our
  // database
  // this method resets the column defs and the query definition to only include columns that are not omitted.
  public void removeOmittedColumns() {
    List<CSVFileColumnDef> currentColumnDefs = this.columnDefs;
    this.columnDefs = new ArrayList<>(columnDefs.size());
    for (CSVFileColumnDef columnDef : currentColumnDefs) {
      if (!columnDef.isOmitted()) {
        this.columnDefs.add(columnDef);
      }
    }
    setNameToColumnDefMap(this.columnDefs);
    setQueryDefinition(dataTableName);
  }

  public static CSVFileDefinition convertXMLToFileDefinition(InputStream inputStream, DataExtractor extractor) throws SAXException, IOException, ParserConfigurationException {
    parseXMLToFileDefinition(inputStream, extractor);
    return extractor.getAllCSVFileDefinitions().get(0);
  }

  private static void parseXMLToFileDefinition(InputStream inputStream, DataExtractor extractor) throws SAXException, IOException, ParserConfigurationException {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setValidating(false);
    SAXParser parser = factory.newSAXParser();
    parser.parse(inputStream, new ConfigParserHandler(extractor));
  }

  /****************************************************************************************************************************************************/
  // methods for unpacking the JSON into the file definition
  public static List<CSVFileDefinition> convertResponseBodyToCSVFileDefinitions(InputStream stream) {
    GsonBuilder gsonBuilder = new GsonBuilder();
    gsonBuilder.registerTypeAdapter(ColumnDef.class, ColumnDef.createJSONDeserializer());
    gsonBuilder.registerTypeAdapter(CSVFileColumnDef.class, CSVFileColumnDef.createCSVJSONDeserializer());
    Gson gson = gsonBuilder.create();
    CSVFileDefinitionJSON vals = gson.fromJson(new InputStreamReader(stream), CSVFileDefinitionJSON.class);
    List<CSVFileDefinition> fileDefinitions;
    if (vals == null || vals.getFileDefinitions() == null) {
      return new ArrayList<>();
    } else {
      fileDefinitions = vals.getFileDefinitions();
    }
    for (CSVFileDefinition fileDefinition : fileDefinitions) {
      fileDefinition.setNameToColumnDefMap(fileDefinition.columnDefs);
    }
    return fileDefinitions;
  }

  public String getAsJSON() {
    CSVFileDefinitionJSON result = new CSVFileDefinitionJSON();
    List<CSVFileDefinition> definitions = new ArrayList<> ();
    definitions.add(this);
    result.setFileDefinitions(definitions);
    return new Gson().toJson(result);
  }

  // Currently only used by tests, returns the index of the column in the csv file that corresponds to the column name
  public int getColumnIndexForColumnName(String nameToFind) {
    int index = 0;
    for (String name : unaliasedNamesInHeaderRowOrder) {
      if (getColumnDefForColumnName(name).nameMatches(nameToFind)) {
        return index;
      }
      index++;
    }
    return -1;
  }

  /****************************************************************************************************************************************************/
  // methods for instantiating the file def
  private void setNameToColumnDefMap(List<CSVFileColumnDef> columnDefs) {
    if (columnNameToColumnDefMap == null) {
      columnNameToColumnDefMap = new HashMap<>(columnDefs.size());
    }
    columnNameToColumnDefMap.clear();
    for (CSVFileColumnDef columnDef : columnDefs) {
        columnNameToColumnDefMap.put(columnDef.getName(), columnDef);
    }
  }

  private void setQueryDefinition(String dataTableName) {
    queryDefinition =  new QueryDefinition();
    queryDefinition.setName(dataTableName);
    for (CSVFileColumnDef columnDef : columnDefs) {
      queryDefinition.addColumn(columnDef);
      columnNameToColumnDefMap.put(columnDef.getName(), columnDef);
    }
  }

  private void setColumnDefs(String dataTableName, List<CSVFileColumnDef> columnDefs) {
    this.columnDefs = columnDefs;
    setNameToColumnDefMap(columnDefs);
    setQueryDefinition(dataTableName);
  }

  /****************************************************************************************************************************************************/
  // methods for validating and parsing data files

  // returns true if there are no missing columns
  private boolean validateAllRequiredColumnsArePresent(List<String> unaliasedNames, DataExtractor extractor) {
    List<String> requiredColumnNames = getRequiredColumnNamesFromColumnDefs();
    Set<String> unaliasedNameSet = new HashSet<>(unaliasedNames);
    StringBuilder missingRequiredColumnsError = null;
    for (String name : requiredColumnNames) {
      if (!unaliasedNameSet.contains(name)) {
        if (missingRequiredColumnsError == null) {
          missingRequiredColumnsError = new StringBuilder("The file is missing required columns : ");
        } else {
          missingRequiredColumnsError.append(", ");
        }
        missingRequiredColumnsError.append("[")
                .append(name)
                .append("]");
      }
    }
    if (missingRequiredColumnsError != null) {
      extractor.getDataExtractorLog().error(missingRequiredColumnsError.toString());
      return false;
    }
    return true;
  }

  private List<String> getRequiredColumnNamesFromColumnDefs() {
    List<String> requiredNames = new ArrayList<>();
    for (CSVFileColumnDef columnDef : columnDefs) {
      if (columnDef.isRequired()) {
        requiredNames.add(columnDef.getName());
      }
    }
    return requiredNames;
  }

  private boolean convertAliasesToNames(Map<String, Integer> columnNames, DataExtractor extractor) {
    boolean noUnmatchedColumns = true;
    unaliasedNamesInHeaderRowOrder = new ArrayList<>(columnNames.size());
    StringBuilder unmatchedColumnsError = new StringBuilder();
    for (Map.Entry<String, Integer> columnName : columnNames.entrySet()) {
      CSVFileColumnDef columnDefForNameOrAlias = findColumnDefForNameOrAlias(columnName.getKey(), unmatchedColumnsError);
      if (columnDefForNameOrAlias != null) {
        unaliasedNamesInHeaderRowOrder.add(columnDefForNameOrAlias.getName());
      } else {
        noUnmatchedColumns = false;
        if (unmatchedColumnsError.length() > 0) {
          unmatchedColumnsError.append(",");
        } else {
          unmatchedColumnsError.append("Unmatched Columns found with the following column headings : ");
        }
        unmatchedColumnsError.append(columnName.getKey())
                .append(" at column position ")
                .append(columnName.getValue());
      }
    }
    if (!noUnmatchedColumns) {
      extractor.getDataExtractorLog().error(unmatchedColumnsError.toString());
    }
    return noUnmatchedColumns;
  }

  private CSVFileColumnDef findColumnDefForNameOrAlias(String columnName, StringBuilder unmatchedColumnsError) {
    for (CSVFileColumnDef columnDef : columnDefs) {
      if (columnDef.nameMatches(columnName) ||
              columnDef.aliasMatches(columnName))
        return columnDef;
    }
    if (unmatchedColumnsError.length() == 0) {
      unmatchedColumnsError = new StringBuilder("The file contains columns that are not recognized for this filetype : ");
    } else {
      unmatchedColumnsError.append(", ");
    }
    unmatchedColumnsError.append("[")
            .append(columnName)
            .append("]");

    return null;
  }

  public int getNumberOfColumnsInHeaderRow() {
    return unaliasedNamesInHeaderRowOrder.size();
  }

  public String getAsXML() {
    StringBuilder sb = new StringBuilder();
    sb.append("<csvFileDefinitions>\n")
            .append('\t')
            .append("<csvfiledef datatablename=\"").append(dataTableName)
            .append("\" version=\"1.0\">\n");
    for (CSVFileColumnDef columnDef : getColumnDefs()) {
      sb.append('\t').append('\t')
              .append("<csvcolumn name=\"").append(columnDef.getActualColumnDef().getName())
              .append("\" type=\"").append(columnDef.getActualColumnDef().getType());
      if (columnDef.getActualColumnDef().getFormatString() != null) {
        sb.append("\" dateformat=\"").append(columnDef.getActualColumnDef().getFormatString());
      }
      sb.append("\" alias=\"").append(columnDef.getAlias())
              .append("\" columnstatus=\"").append(columnDef.getColumnStatus())
              .append("\"/>\n");

    }
    sb.append('\t');
    sb.append("</csvfiledef>\n");
    sb.append("</csvFileDefinitions>");
    return sb.toString();
  }
}

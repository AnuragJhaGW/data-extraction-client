package com.guidewire.tools.benchmarking;

import com.guidewire.tools.MiscUtils;
import org.apache.commons.lang.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.guidewire.tools.MiscUtils.keyValueString;


/**
 * Called by SAXParser when parsing config.xml.  Sets values on a DataExtractor.
 */
@SuppressWarnings("WeakerAccess")
class ConfigParserHandler extends DefaultHandler {

  // Elements and attributes.  I made these public to emphasize that they're part of the contract with the
  // user, i.e. we can't just go and change them or we might break a customer's config file.
  public static final String CONFIG_ELEMENT = "config";

  public static final String CUSTOMER_ELEMENT = "customer";
  public static final String CUSTOMER_ATTR_USERNAME = "userName";
  public static final String CUSTOMER_ATTR_NAME = "name";
  public static final String CUSTOMER_ATTR_ID = "id";

  public static final String GUIDEWIRE_ATTR_OKTA_HOST = "oktaHost";
  public static final String GUIDEWIRE_ATTR_OKTA_ID = "oktaClientId";
  public static final String GUIDEWIRE_ATTR_OKTA_SECRET = "oktaClientSecret";

  public static final String DATABASE_ELEMENT = "database";
  public static final String DATABASE_ATTR_URL = "url";
  public static final String DATABASE_ATTR_USER = "user";
  public static final String DATABASE_ATTR_DB = "db";
  public static final String DATABASE_ATTR_PASSWORD = "password";
  public static final String DATABASE_ATTR_DBTYPE = "dbtype";
  public static final String DATABASE_ATTR_DB_TAG = "db_tag";
  public static final String DATABASE_ATTR_ORACLE_SCHEMA = "oracle_schema";
  public static final String DATABASE_ATTR_ENCRYPT = "encrypt";

  public static final String GUIDEWIRE_ELEMENT = "guidewire";
  public static final String GUIDEWIRE_ATTR_USERNAME = "userName";
  public static final String GUIDEWIRE_ATTR_URL = "url";
  public static final String GUIDEWIRE_ATTR_PROXY_HOST = "proxyHost";
  public static final String GUIDEWIRE_ATTR_PROXY_PORT = "proxyPort";
  public static final String GUIDEWIRE_ATTR_PROXY_AUTHENTICATION_SCHEMA = "proxyAuthenticationSchema";
  public static final String GUIDEWIRE_ATTR_PROXY_USERNAME = "proxyUsername";
  public static final String GUIDEWIRE_ATTR_PROXY_PASSWORD = "proxyPassword";
  public static final String GUIDEWIRE_ATTR_PROXY_DOMAIN = "proxyDomain";
  public static final String GUIDEWIRE_ATTR_CAS_HOST = "casHost";
  public static final String GUIDEWIRE_ATTR_DOMAIN = "domain";
  public static final String GUIDEWIRE_ATTR_PASSWORD = "password";
  public static final String GUIDEWIRE_ATTR_GWAUTH = "gwAuth";

  public static final String QUERY_CONFIG_FILE_ELEMENT = "query_config_file";
  public static final String QUERY_CONFIG_FILE_ATTR_NAME = "name";
  public static final String QUERY_CONFIG_FILE_ATTR_DIR = "dir";

  public static final String CCINFO_ELEMENT = "ccinfo";
  public static final String CCINFO_ATTR_VERSION = "version";

  public static final String TYPECODE_QUERY_ELEMENT = "typecode_query";
  public static final String TYPECODE_QUERY_ATTR_NAME = "name";
  public static final String TYPECODE_QUERY_ATTR_EXCLUDELIST = "excludeList";
  public static final String TYPECODE_QUERY_ATTR_EXCLUDE = "exclude";

  public static final String QUERYDEFINITIONS_ELEMENT = "queryDefinitions";

  public static final String QUERY_ELEMENT = "query";
  public static final String QUERY_ATTR_NAME = "name";
  public static final String QUERY_ATTR_VERSION = "version";
  public static final String QUERY_ATTR_CATCHUP = "catchup";
  public static final String QUERY_ATTR_DAYS_PER_CHUNK = "days_per_chunk";
  public static final String QUERY_ATTR_INCREMENTAL = "incremental";
  public static final String QUERY_ATTR_LAKE_ONLY = "lakeOnly";
  public static final String QUERY_ATTR_EXCLUDE = "exclude";
  public static final String QUERY_ATTR_EARLIEST_DATE = "earliest_date"; // not used

  public static final String COUNTSQL_ELEMENT = "countSQL";
  public static final String SQL_ELEMENT = "sql";

  public static final String CHECKSUMSQL_ELEMENT = "checksumSQL";

  public static final String CSVFILEDEFINITIONS_ELEMENT = "csvFileDefinitions"; // seems to just be used by tests

  public static final String CSVFILEDEF_ELEMENT = "csvfiledef";
  public static final String CSVFILEDEF_ATTR_DATATABLENAME = "datatablename"; // seems to just be used by tests
  public static final String CSVFILEDEF_ATTR_VERSION = "version"; // seems to just be used by tests

  public static final String COLUMN_ELEMENT = "column";
  public static final String COLUMN_ATTR_NAME = "name";
  public static final String COLUMN_ATTR_TYPE = "type";

  public static final String CSVCOLUMN_ELEMENT = "csvcolumn";
  public static final String CSVCOLUMN_ATTR_COLUMNSTATUS = "columnstatus";
  public static final String CSVCOLUMN_ATTR_NAME = "name";
  public static final String CSVCOLUMN_ATTR_TYPE = "type";
  public static final String CSVCOLUMN_ATTR_DATEFORMAT = "dateformat";
  public static final String CSVCOLUMN_ATTR_ALIAS = "alias";

  public static final String WORKING_DIRECTORY_ELEMENT = "working_directory";
  public static final String WORKING_DIRECTORY_ATTR_DIR = "dir";

  public static final String LOGS_ELEMENT = "logs";
  public static final String LOGS_ATTR_DIR = "dir";



  /**
   * Map element names to the set of allowed attributes
   */
  private static final Map<String, Set<String>> _elementToAttributesMap = AttributeHelper.makeElementToAttributesMap();


  private final DataExtractor _dataExtractor;
  private DataExtractorLog _log;
  private QueryDefinition _currentQueryDefinition;
  private CSVFileDefinition _currentCSVFileDefinition;
  private String _currentElement = "";
  private StringBuffer _currentSql;


  ConfigParserHandler(DataExtractor dataExtractor) {
    _dataExtractor = dataExtractor;
    _log = _dataExtractor.getDataExtractorLog();
  }

  public void startDocument() {
    
  }

  public void endDocument() {
    
  }

  public void startElement(String uri, String name, String qName, Attributes atts) {
    _currentElement = qName;

    AttributeHelper attributeHelper = new AttributeHelper(qName, atts);

    _currentSql = new StringBuffer();

    ////////////////       config.xml

    if (attributeHelper.elementEquals(DATABASE_ELEMENT)) {
      _dataExtractor.setDBURL(attributeHelper.get(DATABASE_ATTR_URL));
      _dataExtractor.setDBUserID(attributeHelper.get(DATABASE_ATTR_USER));
      _dataExtractor.setDB(attributeHelper.get(DATABASE_ATTR_DB));
      _dataExtractor.setDBUserPassword(attributeHelper.get(DATABASE_ATTR_PASSWORD));
      _dataExtractor.setDBType(attributeHelper.get(DATABASE_ATTR_DBTYPE));
      if (attributeHelper.has(DATABASE_ATTR_DB_TAG)) {
        _dataExtractor.setDBTag(attributeHelper.get(DATABASE_ATTR_DB_TAG));
      }
      // Note that "oracle_schema" is replacing "db_tag", but we
      // will continue to support "db_tag" for the purposes of
      // backward compatibility.
      if (attributeHelper.has(DATABASE_ATTR_ORACLE_SCHEMA)) {
        _dataExtractor.setDBTag(attributeHelper.get(DATABASE_ATTR_ORACLE_SCHEMA));
      }

      // If the encrypt attribute is set to true, we must decrypt the DB user password
      if (attributeHelper.has(DATABASE_ATTR_ENCRYPT) && attributeHelper.get(DATABASE_ATTR_ENCRYPT).equalsIgnoreCase("true")) {
        _dataExtractor.decryptDBUserPassword();
      }
    }

    else if (attributeHelper.elementEquals(CUSTOMER_ELEMENT)) {
      if (attributeHelper.has(CUSTOMER_ATTR_USERNAME)) {
        _dataExtractor.setUsername(attributeHelper.get(CUSTOMER_ATTR_USERNAME));    // used for connecting to upload
      }
//      _dataExtractor.setName(attributeHelper.get(CUSTOMER_ATTR_NAME));            // not doing much with this
//      _dataExtractor.setCustomerID(attributeHelper.get(CUSTOMER_ATTR_ID));        // used in building individual queries, but rarely
    }

    else if (attributeHelper.elementEquals(GUIDEWIRE_ELEMENT)) {
      if (attributeHelper.has(GUIDEWIRE_ATTR_USERNAME)) {
        _dataExtractor.setUsername(attributeHelper.get(GUIDEWIRE_ATTR_USERNAME));    // used for connecting to upload
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_URL)) {
        _dataExtractor.setUploadURL(attributeHelper.get(GUIDEWIRE_ATTR_URL));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_PROXY_HOST)) {
        _dataExtractor.setProxyHost(attributeHelper.get(GUIDEWIRE_ATTR_PROXY_HOST));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_PROXY_PORT)) {
        _dataExtractor.setProxyPort(attributeHelper.get(GUIDEWIRE_ATTR_PROXY_PORT));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_PROXY_AUTHENTICATION_SCHEMA)) {
        _dataExtractor.setProxyAuthenticationSchema(attributeHelper.get(GUIDEWIRE_ATTR_PROXY_AUTHENTICATION_SCHEMA));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_PROXY_USERNAME)) {
        _dataExtractor.setProxyUsername(attributeHelper.get(GUIDEWIRE_ATTR_PROXY_USERNAME));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_PROXY_PASSWORD)) {
        _dataExtractor.setProxyPassword(attributeHelper.get(GUIDEWIRE_ATTR_PROXY_PASSWORD));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_PROXY_DOMAIN)) {
        _dataExtractor.setProxyDomain(attributeHelper.get(GUIDEWIRE_ATTR_PROXY_DOMAIN));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_CAS_HOST)) {
        _dataExtractor.setCasHost(attributeHelper.get(GUIDEWIRE_ATTR_CAS_HOST));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_DOMAIN)) {
        _dataExtractor.setDomain(attributeHelper.get(GUIDEWIRE_ATTR_DOMAIN));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_OKTA_HOST)) {
        _dataExtractor.setOktaHost(attributeHelper.get(GUIDEWIRE_ATTR_OKTA_HOST));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_OKTA_ID)) {
        _dataExtractor.setOktaClientId(attributeHelper.get(GUIDEWIRE_ATTR_OKTA_ID));
      }
      if (attributeHelper.has(GUIDEWIRE_ATTR_OKTA_SECRET)) {
        _dataExtractor.setOktaClientSecret(attributeHelper.get(GUIDEWIRE_ATTR_OKTA_SECRET));
      }
      _dataExtractor.setGuidewirePassword(attributeHelper.get(GUIDEWIRE_ATTR_PASSWORD));
      _dataExtractor.setGwAuth(attributeHelper.get(GUIDEWIRE_ATTR_GWAUTH));
    }

    else if (attributeHelper.elementEquals(QUERY_CONFIG_FILE_ELEMENT)) {
      _dataExtractor.setQueryConfigFile(new File(attributeHelper.get(QUERY_CONFIG_FILE_ATTR_DIR), attributeHelper.get(QUERY_CONFIG_FILE_ATTR_NAME)));
      _dataExtractor.setQueryConfigFileName(attributeHelper.get(QUERY_CONFIG_FILE_ATTR_NAME));
    }

    else if (attributeHelper.elementEquals(CCINFO_ELEMENT)) {
      _dataExtractor.setCCVersion(attributeHelper.get(CCINFO_ATTR_VERSION));
    }

    else if (attributeHelper.elementEquals(WORKING_DIRECTORY_ELEMENT)) {
      if (attributeHelper.has(WORKING_DIRECTORY_ATTR_DIR)) {
        _dataExtractor.setOutputDir(attributeHelper.get(WORKING_DIRECTORY_ATTR_DIR));
      }
    }

    else if (attributeHelper.elementEquals(LOGS_ELEMENT)) {
      if (attributeHelper.has(LOGS_ATTR_DIR)) {
        _dataExtractor.getDataExtractorLog().setLogsDir(attributeHelper.get(LOGS_ATTR_DIR));
      }
    }

    /////////////////////////////   query xml files

    else if (attributeHelper.elementEquals(QUERY_ELEMENT)) {
      _currentQueryDefinition = new QueryDefinition();
      _currentQueryDefinition.setName(attributeHelper.get(QUERY_ATTR_NAME));
      _currentQueryDefinition.setVersion(attributeHelper.get(QUERY_ATTR_VERSION));

      if ("true".equalsIgnoreCase(attributeHelper.get(QUERY_ATTR_CATCHUP))) {
        _currentQueryDefinition.setCatchUp(true);
      }

      if (attributeHelper.has(QUERY_ATTR_DAYS_PER_CHUNK)) {
        _currentQueryDefinition.setChunk(true);
        _currentQueryDefinition.setDaysPerChunk(attributeHelper.get(QUERY_ATTR_DAYS_PER_CHUNK));
      }

      if (attributeHelper.has(QUERY_ATTR_INCREMENTAL)) {
        boolean incremental = attributeHelper.get(QUERY_ATTR_INCREMENTAL).trim().equalsIgnoreCase("true");
        _currentQueryDefinition.setIncremental(incremental);
      }

      if (attributeHelper.has(QUERY_ATTR_LAKE_ONLY)) {
        boolean lakeOnly = attributeHelper.get(QUERY_ATTR_LAKE_ONLY).trim().equalsIgnoreCase("true");
        _currentQueryDefinition.setLakeOnly(lakeOnly);
      }

      if (attributeHelper.has(QUERY_ATTR_EXCLUDE)) {
        setExclusions(attributeHelper.get(QUERY_ATTR_EXCLUDE));
      }
    }

    else if (attributeHelper.elementEquals(COLUMN_ELEMENT)) {
      ColumnDef column = ColumnDef.createDefinition(attributeHelper.get(COLUMN_ATTR_TYPE), attributeHelper.get(COLUMN_ATTR_NAME));
      _currentQueryDefinition.addColumn(column);
    }

    else if (attributeHelper.elementEquals(TYPECODE_QUERY_ELEMENT)) {
      _currentQueryDefinition = QueryDefinition.createBaseTypecodeQuery(attributeHelper.get(TYPECODE_QUERY_ATTR_NAME),
        attributeHelper.get(TYPECODE_QUERY_ATTR_EXCLUDELIST));
      if (attributeHelper.has(TYPECODE_QUERY_ATTR_EXCLUDE)) {
        setExclusions(attributeHelper.get(TYPECODE_QUERY_ATTR_EXCLUDE));
      }
    }


    /////////////////////////////     csv file defs

    else if (attributeHelper.elementEquals(CSVFILEDEF_ELEMENT)) {
        _currentCSVFileDefinition = new CSVFileDefinition(attributeHelper.get(CSVFILEDEF_ATTR_DATATABLENAME));
    }

    else if (attributeHelper.elementEquals(CSVCOLUMN_ELEMENT)){
      String columnstatus = attributeHelper.get(CSVCOLUMN_ATTR_COLUMNSTATUS);
      CSVFileColumnDef.ColumnStatus status = columnstatus.toLowerCase().equals("required") ? CSVFileColumnDef.ColumnStatus.REQUIRED :
              columnstatus.toLowerCase().equals("omitted") ? CSVFileColumnDef.ColumnStatus.OMITTED : CSVFileColumnDef.ColumnStatus.NOTREQUIRED;
       CSVFileColumnDef columnDef = new CSVFileColumnDef(attributeHelper.get(CSVCOLUMN_ATTR_NAME), attributeHelper.get(CSVCOLUMN_ATTR_TYPE),
               attributeHelper.get(CSVCOLUMN_ATTR_DATEFORMAT), attributeHelper.get(CSVCOLUMN_ATTR_ALIAS), status);
      _currentCSVFileDefinition.addColumnDef(columnDef);
    }
  }


  public void startPrefixMapping(String prefix, String uri) {
    
    
  }

  public void endPrefixMapping(String prefix) {
    
  }

  public void characters(char[] ch, int start, int length) {
    if (start == 0) {
      // in JDK 1.8.0_201 there's a weird behavior where this method gets called with some extra characters.
      if (_currentSql.length() > 0 || !new String(ch, start, length).trim().isEmpty()) {
        _currentSql.append(ch, 0, length);
      }

      if ("countSQL".equals(_currentElement)) {
        _currentQueryDefinition.setCountSQL(_currentSql.toString());
        
      }
      if ("sql".equals(_currentElement)) {
        _currentQueryDefinition.setOriginalSQL(_currentSql.toString());
//        _currentQueryDefinition.setOriginalSQL(new String(ch));
        
      }
      if ("checksumSQL".equals(_currentElement)) {
        _currentQueryDefinition.setChecksumSQL(_currentSql.toString());
        
      }
    }
  }

  public void endElement(String uri, String name, String qName) {
    XmlHelper xmlHelper = new XmlHelper(qName);

    if (xmlHelper.elementEquals(QUERY_ELEMENT)) {
      Pattern selectFound = Pattern.compile("SELECT|select");
      if (!selectFound.matcher(_currentQueryDefinition.getOriginalSQL()).find()) {
        for (QueryDefinition query : _dataExtractor.getAllQueries()) {
          _log.info("query definition: " + query.getName());
          _log.info("original " + query.getOriginalSQL());
          _log.info("transformed " + query.getTransformedSQL(_dataExtractor.getDBTag(), _dataExtractor.getDB()));
          _log.info("count " + query.getCountSQL());
          _log.info("version " + query.getVersion());
        }
        _log.info("Problem with query definition: " + _currentQueryDefinition.getName());
        _log.info("original " + _currentQueryDefinition.getOriginalSQL());
        _log.info("transformed " + _currentQueryDefinition.getTransformedSQL(_dataExtractor.getDBTag(), _dataExtractor.getDB()));
        _log.info("version " + _currentQueryDefinition.getVersion());
        _log.info("count " + _currentQueryDefinition.getCountSQL());
        throw new RuntimeException("invalid query def");
      }
      _dataExtractor.addQuery(_currentQueryDefinition);
      _currentQueryDefinition = null;
    }
    if (xmlHelper.elementEquals(CSVFILEDEF_ELEMENT)) {
      _dataExtractor.addCSVFileDefinition(_currentCSVFileDefinition);
      _currentCSVFileDefinition = null;
    }
    if (xmlHelper.elementEquals(TYPECODE_QUERY_ELEMENT)) {
      _dataExtractor.addQuery(_currentQueryDefinition);
      _currentQueryDefinition = null;
    }
    _currentElement = "";
  }


  void writeQueryConfigFileTo(FileWriter out) throws IOException {
    out.append("<" + QUERYDEFINITIONS_ELEMENT + ">\n");
    for (QueryDefinition query : _dataExtractor.getAllQueries()) {
      out.append("  <" + QUERY_ELEMENT + " " + QUERY_ATTR_NAME + "=\"" + query.getName() + "\" " + QUERY_ATTR_VERSION + "=\"" + query.getVersion() + "\"");
      if (query.isChunk()) {
        if (query.getDaysForEachChunk() > 0) {
          out.append(" " + QUERY_ATTR_DAYS_PER_CHUNK + "=\"" + query.getDaysForEachChunk() + "\"");
        }
      }
      if (query.isIncremental()) {
        out.append(" " + QUERY_ATTR_INCREMENTAL + "=\"true\"");
      }
      if (query.hasExclusions()) {
        out.append(" " + QUERY_ATTR_EXCLUDE + "=\"");
        boolean firstExclusion = true;
        for (Integer ccVersion : query.getVersionsToExclude()) {
          if (firstExclusion) {
            firstExclusion = false;
          } else {
            out.append(",");
          }
          out.append("CC" + ccVersion);
        }
        out.append("\"");
      }
      out.append(">\n");
      if (query.getCountSQL() != null) {
        out.append("    <" + COUNTSQL_ELEMENT + "><![CDATA[");
        out.append(query.getCountSQL());
        out.append("]]>");
        out.append("    </" + COUNTSQL_ELEMENT + ">\n");
      }
      out.append("    <" + SQL_ELEMENT + ">\n");
      out.append("      <![CDATA[");
      out.append(query.getTransformedSQL("&DB_TAG", "&DB_NAME"));
      out.append("]]>");
      out.append("    </" + SQL_ELEMENT + ">\n");
      for (ColumnDef column : query.getColumns()) {
        out.append("    <" + COLUMN_ELEMENT + " " + COLUMN_ATTR_NAME + "=\"" + column.getName() + "\"");
        out.append(" " + COLUMN_ATTR_TYPE + "=\"" + column.getType() + "\"/>\n");
      }
      out.append("  </" + QUERY_ELEMENT + ">\n\n");
    }
    out.append("</" + QUERYDEFINITIONS_ELEMENT + ">");
    out.close();
  }

  //////////////////////////////    private


  /**
   * Helper for working with XML
   */
  private static class XmlHelper {
    final String _element;

    XmlHelper(String element) {
      _element = element;
    }

    /**
     * Returns true if _element == element, case-insensitively.
     */
    boolean elementEquals(String element) {
      return _element.toLowerCase().equals(element.toLowerCase());
    }
  }


  /**
   * Helps us define our allowed attributes and get values in a case insensitive way
   */
  private static class AttributeHelper extends XmlHelper {
    private final Set<String> _allowedAttributeNames;
    private final Attributes _attributes;

    AttributeHelper(String element, Attributes attributes) {
      super(element);
      _attributes = attributes;
      _allowedAttributeNames = _elementToAttributesMap.get(element.toLowerCase());

      // validate
      if (_allowedAttributeNames == null) {
        throw new RuntimeException("Bad element: " + element);
      }
      Set<String> badAttributes = getBadAttributes();
      if (!badAttributes.isEmpty()) {
        String sBadAttributes = StringUtils.join(badAttributes, ',');
        throw new RuntimeException("Bad attribute(s) for element '" + _element + "': " + sBadAttributes);
      }
    }


    /**
     * Returns true if there is an attribute value for the given name.
     */
    boolean has(String attribute) {
      return get(attribute) != null;
    }


    /**
     * Returns the attribute value.  Name is case insensitive.  Returns null if the attribute isn't in _attributes.
     */
    String get(String attribute) {
      if (!_allowedAttributeNames.contains(attribute.toLowerCase())) {
        throw new RuntimeException("Bad attribute name for element: " + keyValueString("element", _element, "attribute", attribute));
      }

      for (int i = 0; i < _attributes.getLength(); i++) {
        if (_attributes.getLocalName(i).toLowerCase().equals(attribute.toLowerCase())) {
          return _attributes.getValue(i);
        }
      }

      return null;
    }


    /**
     * Returns a set of attributes name from attributes which aren't allowed for the given element.
     */
    private Set<String> getBadAttributes() {
      Set<String> badAttributeNames = new HashSet<>();
      for (int i = 0; i < _attributes.getLength(); i++) {
        String attributeName = _attributes.getLocalName(i);
        if (!_allowedAttributeNames.contains(attributeName.toLowerCase())) {
          badAttributeNames.add(attributeName);
        }
      }

      return badAttributeNames;
    }


    /**
     * Create map from elements to allowed attributes
     */
    static Map<String, Set<String>> makeElementToAttributesMap() {
      Map<String, Set<String>> map = new HashMap<>();

      addElementAndAttributes(map, CONFIG_ELEMENT);

      addElementAndAttributes(map, CUSTOMER_ELEMENT,
        CUSTOMER_ATTR_USERNAME,
        CUSTOMER_ATTR_NAME,
        CUSTOMER_ATTR_ID);

      addElementAndAttributes(map, DATABASE_ELEMENT,
        DATABASE_ATTR_URL,
        DATABASE_ATTR_USER,
        DATABASE_ATTR_DB,
        DATABASE_ATTR_PASSWORD,
        DATABASE_ATTR_DBTYPE,
        DATABASE_ATTR_DB_TAG,
        DATABASE_ATTR_ORACLE_SCHEMA,
        DATABASE_ATTR_ENCRYPT);

      addElementAndAttributes(map, GUIDEWIRE_ELEMENT,
        GUIDEWIRE_ATTR_USERNAME,
        GUIDEWIRE_ATTR_URL,
        GUIDEWIRE_ATTR_PROXY_HOST,
        GUIDEWIRE_ATTR_PROXY_PORT,
        GUIDEWIRE_ATTR_PROXY_AUTHENTICATION_SCHEMA,
        GUIDEWIRE_ATTR_PROXY_USERNAME,
        GUIDEWIRE_ATTR_PROXY_PASSWORD,
        GUIDEWIRE_ATTR_PROXY_DOMAIN,
        GUIDEWIRE_ATTR_CAS_HOST,
        GUIDEWIRE_ATTR_DOMAIN,
        GUIDEWIRE_ATTR_PASSWORD,
        GUIDEWIRE_ATTR_GWAUTH,
        GUIDEWIRE_ATTR_OKTA_HOST,
        GUIDEWIRE_ATTR_OKTA_ID,
        GUIDEWIRE_ATTR_OKTA_SECRET
              );

      addElementAndAttributes(map, QUERY_CONFIG_FILE_ELEMENT,
        QUERY_CONFIG_FILE_ATTR_NAME,
        QUERY_CONFIG_FILE_ATTR_DIR);

      addElementAndAttributes(map, CCINFO_ELEMENT, CCINFO_ATTR_VERSION);

      addElementAndAttributes(map, QUERYDEFINITIONS_ELEMENT);

      addElementAndAttributes(map, TYPECODE_QUERY_ELEMENT,
        TYPECODE_QUERY_ATTR_NAME,
        TYPECODE_QUERY_ATTR_EXCLUDELIST,
        TYPECODE_QUERY_ATTR_EXCLUDE);

      addElementAndAttributes(map, QUERY_ELEMENT,
        QUERY_ATTR_NAME,
        QUERY_ATTR_VERSION,
        QUERY_ATTR_CATCHUP,
        QUERY_ATTR_DAYS_PER_CHUNK,
        QUERY_ATTR_INCREMENTAL,
        QUERY_ATTR_EXCLUDE,
              QUERY_ATTR_EARLIEST_DATE,
              QUERY_ATTR_LAKE_ONLY);

      addElementAndAttributes(map, COUNTSQL_ELEMENT);
      addElementAndAttributes(map, SQL_ELEMENT);
      addElementAndAttributes(map, CSVFILEDEFINITIONS_ELEMENT);
      addElementAndAttributes(map, CHECKSUMSQL_ELEMENT);

      addElementAndAttributes(map, CSVFILEDEF_ELEMENT,
        CSVFILEDEF_ATTR_DATATABLENAME,
        CSVFILEDEF_ATTR_VERSION);

      addElementAndAttributes(map, COLUMN_ELEMENT,
        COLUMN_ATTR_NAME,
        COLUMN_ATTR_TYPE);

      addElementAndAttributes(map, CSVCOLUMN_ELEMENT,
        CSVCOLUMN_ATTR_COLUMNSTATUS,
        CSVCOLUMN_ATTR_NAME,
        CSVCOLUMN_ATTR_TYPE,
        CSVCOLUMN_ATTR_DATEFORMAT,
        CSVCOLUMN_ATTR_ALIAS);

      addElementAndAttributes(map, WORKING_DIRECTORY_ELEMENT, WORKING_DIRECTORY_ATTR_DIR);
      addElementAndAttributes(map, LOGS_ELEMENT, LOGS_ATTR_DIR);

      return map;
    }


    /**
     * Add to the map.  Everything is converted to lower case
     */
    static private void addElementAndAttributes(Map<String, Set<String>> map, String element, String... attributes) {
      Set<String> attributeSet = new HashSet<>();
      for (String attribute : attributes) {
        attributeSet.add(attribute.toLowerCase());
      }
      map.put(element.toLowerCase(), attributeSet);
    }
  }


  /**
   * Sets versionsToExclude on _currentQueryDefinition
   * @param ccExcludedVersions Comma separated list of cc versions: "CC4, CC5"
   */
  private void setExclusions(String ccExcludedVersions) {
    Set<Integer> excludedVersions = Arrays.stream(ccExcludedVersions.split(","))
            .map(MiscUtils::ccVersion)
            .collect(Collectors.toSet());

    _currentQueryDefinition.setVersionsToExclude(excludedVersions);
  }
}

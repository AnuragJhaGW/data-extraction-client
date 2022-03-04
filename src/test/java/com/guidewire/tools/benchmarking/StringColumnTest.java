package com.guidewire.tools.benchmarking;

import com.guidewire.cloudviewer.datamoving.QueryResult;
import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class description...
 */
@Test(groups="unit")
public class StringColumnTest {

  private final Random random = new Random();
  Pattern carriageReturn = Pattern.compile("\r");

  public void testBackslashesEscaped() throws Exception {
    String oneBackslash = "blah \\ blah";
    String twoBackslash = "blah \\\\ blah";
    String threeBackslash = "blah \\\\\\ blah";

    StringColumn column = (StringColumn)ColumnDef.createDefinition(ColumnDef.STRING, "name");
    Assert.assertEquals(oneBackslash, column.unescapeSpecialCharacters(column.escapeSpecialCharacters(oneBackslash)));
    Assert.assertEquals(twoBackslash, column.unescapeSpecialCharacters(column.escapeSpecialCharacters(twoBackslash)));
    Assert.assertEquals(threeBackslash, column.unescapeSpecialCharacters(column.escapeSpecialCharacters(threeBackslash)));
  }

  public void testCSVReaderRoundTripWithRandomCharacterStrings() throws Exception {
    StringWriter stringWriter = new StringWriter();
    CSVWriter csvWriter = new CSVWriter(stringWriter);
    StringColumn column = (StringColumn) ColumnDef.createDefinition(ColumnDef.STRING, "name");
    int rows = 1000;
    int columns = 1000;
    int stringSize = 10;
    List<String[]> strings = new ArrayList<String[]>(rows);
    int row = 0;
    while (row++ < rows) {
      String[] rowData = createRandomRow(columns, stringSize);
      String[] escapedRow = escapeRow(column, rowData);
      strings.add(rowData);
      csvWriter.writeNext(escapedRow);
    }

    CSVReader csvReader = new CSVReader(new StringReader(stringWriter.getBuffer().toString()));
    for (String[] string : strings) {
      String[] actual = csvReader.readNext();
      for (int i=0; i < string.length; i++) {
        String unescapedActual = column.unescapeSpecialCharacters(actual[i]);
        Matcher crs = carriageReturn.matcher(string[i]);
        String expected = crs.replaceAll("\n");
        if (!expected.equals(unescapedActual)) {
          System.out.println(string[i]);
        }
        Assert.assertEquals(unescapedActual, expected);
      }
    }
  }

  private String[] createRandomRow(int columns, int stringSize) {
    String[] result = new String[columns];
    for (int col = 0; col < columns; col++) {
      result[col] = createRandomString(stringSize);
    }
    return result;
  }

  private String[] escapeRow(StringColumn column, String[] row) {
    String[] result = new String[row.length];
    for (int col = 0; col < row.length; col++) {
      result[col] = column.escapeSpecialCharacters(row[col]);
    }
    return result;
  }

  private String createRandomString(int size) {
    StringBuffer buffer = new StringBuffer(size);
    int i = 0;
    while (i++ < size) {
      buffer.append((char) random.nextInt(256));
    }
    return buffer.toString();
  }

  public void testGSONRoundTripWithTrailingSpecialCharacters() throws Exception {
    String trailingComma = "trailing comma,";
    String trailingBackslash = "tailing backslash \\";
    String trailingDoubleBackslash = "tailing backslash \\\\";
    String trailingQuote = "trailing quotes\"";

    QueryResult queryResult = new QueryResult();
    List<ColumnDef> columns = new ArrayList<ColumnDef>(3);
    StringColumn first = (StringColumn) ColumnDef.createDefinition(ColumnDef.STRING, "first");
    columns.add(first);
    columns.add(ColumnDef.createDefinition(ColumnDef.STRING, "second"));
    columns.add(ColumnDef.createDefinition(ColumnDef.STRING, "third"));
//    queryResult.setColumns(columns);

    // make three rows rotating the position of each value.
    QueryResult.ResultRow resultRow = QueryResult.newResultRow(4);
    resultRow.add(first.escapeSpecialCharacters(trailingComma));
    resultRow.add(first.escapeSpecialCharacters(trailingBackslash));
    resultRow.add(first.escapeSpecialCharacters(trailingDoubleBackslash));
    resultRow.add(first.escapeSpecialCharacters(trailingQuote));
    queryResult.addRow(resultRow);
    resultRow = QueryResult.newResultRow(4);
    resultRow.add(first.escapeSpecialCharacters(trailingQuote));
    resultRow.add(first.escapeSpecialCharacters(trailingComma));
    resultRow.add(first.escapeSpecialCharacters(trailingBackslash));
    resultRow.add(first.escapeSpecialCharacters(trailingDoubleBackslash));
    queryResult.addRow(resultRow);
    resultRow = QueryResult.newResultRow(4);
    resultRow.add(first.escapeSpecialCharacters(trailingDoubleBackslash));
    resultRow.add(first.escapeSpecialCharacters(trailingQuote));
    resultRow.add(first.escapeSpecialCharacters(trailingComma));
    resultRow.add(first.escapeSpecialCharacters(trailingBackslash));
    queryResult.addRow(resultRow);
    resultRow = QueryResult.newResultRow(4);
    resultRow.add(first.escapeSpecialCharacters(trailingBackslash));
    resultRow.add(first.escapeSpecialCharacters(trailingDoubleBackslash));
    resultRow.add(first.escapeSpecialCharacters(trailingQuote));
    resultRow.add(first.escapeSpecialCharacters(trailingComma));
    queryResult.addRow(resultRow);

    GsonBuilder gsonBuilder = new GsonBuilder();
    Gson gson = gsonBuilder.create();
    QueryResult sentObject = gson.fromJson(gson.toJson(queryResult), QueryResult.class);

    // Verify that the unescape leaves us with the same string in each case
    resultRow = sentObject.getRows().get(0);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(0)), trailingComma);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(1)), trailingBackslash);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(2)), trailingDoubleBackslash);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(3)), trailingQuote);
    resultRow = sentObject.getRows().get(1);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(0)), trailingQuote);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(1)), trailingComma);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(2)), trailingBackslash);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(3)), trailingDoubleBackslash);
    resultRow = sentObject.getRows().get(2);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(0)), trailingDoubleBackslash);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(1)), trailingQuote);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(2)), trailingComma);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(3)), trailingBackslash);
    resultRow = sentObject.getRows().get(3);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(0)), trailingBackslash);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(1)), trailingDoubleBackslash);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(2)), trailingQuote);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(3)), trailingComma);
  }

  public void testCSVRoundTripWithTrailingSpecialCharacters() throws Exception {
    String trailingComma = "trailing comma,";
    String trailingBackslash = "tailing backslash \\";
    String trailingDoubleBackslash = "tailing double backslash \\\\";
    String trailingQuote = "trailing quotes\"";
    String leadingAndTrailingQuote = "\"leading and trailing quotes\"";

    StringColumn column = (StringColumn) ColumnDef.createDefinition(ColumnDef.STRING, "first");

    StringBuffer csv = new StringBuffer();
    StringWriter stringWriter = new StringWriter();
    CSVWriter writer = new CSVWriter(stringWriter);
    writer.writeNext(new String[] {
            column.escapeSpecialCharacters(trailingComma),
            column.escapeSpecialCharacters(trailingBackslash),
            column.escapeSpecialCharacters(trailingDoubleBackslash),
            column.escapeSpecialCharacters(trailingQuote),
            column.escapeSpecialCharacters(leadingAndTrailingQuote)});
    writer.writeNext(new String[] {
            column.escapeSpecialCharacters(leadingAndTrailingQuote),
            column.escapeSpecialCharacters(trailingComma),
            column.escapeSpecialCharacters(trailingBackslash),
            column.escapeSpecialCharacters(trailingDoubleBackslash),
            column.escapeSpecialCharacters(trailingQuote)});
    writer.writeNext(new String[] {
            column.escapeSpecialCharacters(trailingQuote),
            column.escapeSpecialCharacters(leadingAndTrailingQuote),
            column.escapeSpecialCharacters(trailingComma),
            column.escapeSpecialCharacters(trailingBackslash),
            column.escapeSpecialCharacters(trailingDoubleBackslash)});
    writer.writeNext(new String[] {
            column.escapeSpecialCharacters(trailingDoubleBackslash),
            column.escapeSpecialCharacters(trailingQuote),
            column.escapeSpecialCharacters(leadingAndTrailingQuote),
            column.escapeSpecialCharacters(trailingComma),
            column.escapeSpecialCharacters(trailingBackslash)});
    writer.writeNext(new String[] {
            column.escapeSpecialCharacters(trailingBackslash),
            column.escapeSpecialCharacters(trailingDoubleBackslash),
            column.escapeSpecialCharacters(trailingQuote),
            column.escapeSpecialCharacters(leadingAndTrailingQuote),
            column.escapeSpecialCharacters(trailingComma)});

    CSVReader reader = new CSVReader(new StringReader(stringWriter.toString()));
    String[] first = reader.readNext();
    String[] second = reader.readNext();
    String[] third = reader.readNext();
    String[] fourth = reader.readNext();
    String[] fifth = reader.readNext();

    // Verify that the unescape leaves us with the same string in each case
    Assert.assertEquals(column.unescapeSpecialCharacters(first[0]), trailingComma);
    Assert.assertEquals(column.unescapeSpecialCharacters(first[1]), trailingBackslash);
    Assert.assertEquals(column.unescapeSpecialCharacters(first[2]), trailingDoubleBackslash);
    Assert.assertEquals(column.unescapeSpecialCharacters(first[3]), trailingQuote);
    Assert.assertEquals(column.unescapeSpecialCharacters(first[4]), leadingAndTrailingQuote);

    Assert.assertEquals(column.unescapeSpecialCharacters(second[0]), leadingAndTrailingQuote);
    Assert.assertEquals(column.unescapeSpecialCharacters(second[1]), trailingComma);
    Assert.assertEquals(column.unescapeSpecialCharacters(second[2]), trailingBackslash);
    Assert.assertEquals(column.unescapeSpecialCharacters(second[3]), trailingDoubleBackslash);
    Assert.assertEquals(column.unescapeSpecialCharacters(second[4]), trailingQuote);

    Assert.assertEquals(column.unescapeSpecialCharacters(third[0]), trailingQuote);
    Assert.assertEquals(column.unescapeSpecialCharacters(third[1]), leadingAndTrailingQuote);
    Assert.assertEquals(column.unescapeSpecialCharacters(third[2]), trailingComma);
    Assert.assertEquals(column.unescapeSpecialCharacters(third[3]), trailingBackslash);
    Assert.assertEquals(column.unescapeSpecialCharacters(third[4]), trailingDoubleBackslash);

    Assert.assertEquals(column.unescapeSpecialCharacters(fourth[0]), trailingDoubleBackslash);
    Assert.assertEquals(column.unescapeSpecialCharacters(fourth[1]), trailingQuote);
    Assert.assertEquals(column.unescapeSpecialCharacters(fourth[2]), leadingAndTrailingQuote);
    Assert.assertEquals(column.unescapeSpecialCharacters(fourth[3]), trailingComma);
    Assert.assertEquals(column.unescapeSpecialCharacters(fourth[4]), trailingBackslash);

    Assert.assertEquals(column.unescapeSpecialCharacters(fifth[0]), trailingBackslash);
    Assert.assertEquals(column.unescapeSpecialCharacters(fifth[1]), trailingDoubleBackslash);
    Assert.assertEquals(column.unescapeSpecialCharacters(fifth[2]), trailingQuote);
    Assert.assertEquals(column.unescapeSpecialCharacters(fifth[3]), leadingAndTrailingQuote);
    Assert.assertEquals(column.unescapeSpecialCharacters(fifth[4]), trailingComma);
  }

  public void testGSONRoundTripWithNonLatin1Characters() throws Exception {
    String accents = "Montée-Pélémard";
    String japanese1 = "東京都";
    String japanese2 = "中央区八重洲一丁目5番3号";
    String chineseBig5 = "中文數位化技術推廣委員會";

    QueryResult queryResult = new QueryResult();
    List<ColumnDef> columns = new ArrayList<ColumnDef>(3);
    StringColumn first = (StringColumn) ColumnDef.createDefinition(ColumnDef.STRING, "first");
    columns.add(first);
    columns.add(ColumnDef.createDefinition(ColumnDef.STRING, "second"));
    columns.add(ColumnDef.createDefinition(ColumnDef.STRING, "third"));
//    queryResult.setColumns(columns);

    // make three rows rotating the position of each value.
    QueryResult.ResultRow resultRow = QueryResult.newResultRow(4);
    resultRow.add(first.escapeSpecialCharacters(accents));
    resultRow.add(first.escapeSpecialCharacters(japanese1));
    resultRow.add(first.escapeSpecialCharacters(japanese2));
    resultRow.add(first.escapeSpecialCharacters(chineseBig5));
    queryResult.addRow(resultRow);
    resultRow = QueryResult.newResultRow(4);
    resultRow.add(first.escapeSpecialCharacters(chineseBig5));
    resultRow.add(first.escapeSpecialCharacters(accents));
    resultRow.add(first.escapeSpecialCharacters(japanese1));
    resultRow.add(first.escapeSpecialCharacters(japanese2));
    queryResult.addRow(resultRow);
    resultRow = QueryResult.newResultRow(4);
    resultRow.add(first.escapeSpecialCharacters(japanese2));
    resultRow.add(first.escapeSpecialCharacters(chineseBig5));
    resultRow.add(first.escapeSpecialCharacters(accents));
    resultRow.add(first.escapeSpecialCharacters(japanese1));
    queryResult.addRow(resultRow);
    resultRow = QueryResult.newResultRow(4);
    resultRow.add(first.escapeSpecialCharacters(japanese1));
    resultRow.add(first.escapeSpecialCharacters(japanese2));
    resultRow.add(first.escapeSpecialCharacters(chineseBig5));
    resultRow.add(first.escapeSpecialCharacters(accents));
    queryResult.addRow(resultRow);

    GsonBuilder gsonBuilder = new GsonBuilder();
    Gson gson = gsonBuilder.create();
    String toSend = gson.toJson(queryResult);
    QueryResult sentObject = gson.fromJson(toSend, QueryResult.class);

    // Verify that the unescape leaves us with the same string in each case
    resultRow = sentObject.getRows().get(0);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(0)), accents);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(1)), japanese1);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(2)), japanese2);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(3)), chineseBig5);
    resultRow = sentObject.getRows().get(1);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(0)), chineseBig5);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(1)), accents);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(2)), japanese1);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(3)), japanese2);
    resultRow = sentObject.getRows().get(2);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(0)), japanese2);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(1)), chineseBig5);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(2)), accents);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(3)), japanese1);
    resultRow = sentObject.getRows().get(3);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(0)), japanese1);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(1)), japanese2);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(2)), chineseBig5);
    Assert.assertEquals(first.unescapeSpecialCharacters(resultRow.getResults().get(3)), accents);
  }

  public void testCSVRoundTripWithNonLatinCharacters() throws Exception {
    String accents = "Montée-Pélémard";
    String german = "Füße";
    String japanese1 = "東京都";
    String japanese2 = "中央区八重洲一丁目5番3号";
    String chineseBig5 = "中文數位化技術推廣委員會";

    StringColumn column = (StringColumn) ColumnDef.createDefinition(ColumnDef.STRING, "first");

    StringWriter stringWriter = new StringWriter();
    CSVWriter writer = new CSVWriter(stringWriter);
//    FileWriter out = new FileWriter("testCSV.csv");
//    CSVWriter writer = new CSVWriter(out);
    writer.writeNext(new String[] {
      column.escapeSpecialCharacters(accents),
      column.escapeSpecialCharacters(german),
      column.escapeSpecialCharacters(japanese1),
      column.escapeSpecialCharacters(japanese2),
      column.escapeSpecialCharacters(chineseBig5)});
    writer.writeNext(new String[] {
      column.escapeSpecialCharacters(chineseBig5),
      column.escapeSpecialCharacters(accents),
      column.escapeSpecialCharacters(german),
      column.escapeSpecialCharacters(japanese1),
      column.escapeSpecialCharacters(japanese2)});
    writer.writeNext(new String[] {
      column.escapeSpecialCharacters(japanese2),
      column.escapeSpecialCharacters(chineseBig5),
      column.escapeSpecialCharacters(accents),
      column.escapeSpecialCharacters(german),
      column.escapeSpecialCharacters(japanese1)});
    writer.writeNext(new String[] {
      column.escapeSpecialCharacters(japanese1),
      column.escapeSpecialCharacters(japanese2),
      column.escapeSpecialCharacters(chineseBig5),
      column.escapeSpecialCharacters(accents),
      column.escapeSpecialCharacters(german)});
    writer.writeNext(new String[] {
      column.escapeSpecialCharacters(german),
      column.escapeSpecialCharacters(japanese1),
      column.escapeSpecialCharacters(japanese2),
      column.escapeSpecialCharacters(chineseBig5),
      column.escapeSpecialCharacters(accents)});

//    out.close();

//    CSVReader reader = new CSVReader(new FileReader("testCSV.csv"));
    CSVReader reader = new CSVReader(new StringReader(stringWriter.toString()));
    String[] first = reader.readNext();
    String[] second = reader.readNext();
    String[] third = reader.readNext();
    String[] fourth = reader.readNext();
    String[] fifth = reader.readNext();

    // Verify that the unescape leaves us with the same string in each case
    Assert.assertEquals(column.unescapeSpecialCharacters(first[0]), accents);
    Assert.assertEquals(column.unescapeSpecialCharacters(first[1]), german);
    Assert.assertEquals(column.unescapeSpecialCharacters(first[2]), japanese1);
    Assert.assertEquals(column.unescapeSpecialCharacters(first[3]), japanese2);
    Assert.assertEquals(column.unescapeSpecialCharacters(first[4]), chineseBig5);

    Assert.assertEquals(column.unescapeSpecialCharacters(second[0]), chineseBig5);
    Assert.assertEquals(column.unescapeSpecialCharacters(second[1]), accents);
    Assert.assertEquals(column.unescapeSpecialCharacters(second[2]), german);
    Assert.assertEquals(column.unescapeSpecialCharacters(second[3]), japanese1);
    Assert.assertEquals(column.unescapeSpecialCharacters(second[4]), japanese2);

    Assert.assertEquals(column.unescapeSpecialCharacters(third[0]), japanese2);
    Assert.assertEquals(column.unescapeSpecialCharacters(third[1]), chineseBig5);
    Assert.assertEquals(column.unescapeSpecialCharacters(third[2]), accents);
    Assert.assertEquals(column.unescapeSpecialCharacters(third[3]), german);
    Assert.assertEquals(column.unescapeSpecialCharacters(third[4]), japanese1);

    Assert.assertEquals(column.unescapeSpecialCharacters(fourth[0]), japanese1);
    Assert.assertEquals(column.unescapeSpecialCharacters(fourth[1]), japanese2);
    Assert.assertEquals(column.unescapeSpecialCharacters(fourth[2]), chineseBig5);
    Assert.assertEquals(column.unescapeSpecialCharacters(fourth[3]), accents);
    Assert.assertEquals(column.unescapeSpecialCharacters(fourth[4]), german);

    Assert.assertEquals(column.unescapeSpecialCharacters(fifth[0]), german);
    Assert.assertEquals(column.unescapeSpecialCharacters(fifth[1]), japanese1);
    Assert.assertEquals(column.unescapeSpecialCharacters(fifth[2]), japanese2);
    Assert.assertEquals(column.unescapeSpecialCharacters(fifth[3]), chineseBig5);
    Assert.assertEquals(column.unescapeSpecialCharacters(fifth[4]), accents);
  }
}

package com.guidewire.tools.benchmarking;

import java.util.ArrayList;
import java.util.List;

/**
 * This gets a CSVFileDefinition that contains the changes required for particular customers. The changed columns held in the modifying file
 * definition are applied to the GW base definition to create a customer specific file definition. In our database we store the base GW default
 * definition and customer specific Modifying definitions. Those Modifying definitions only specify the columns that differ from the base definition.
 * The modifier should only ever be instantiated on the server side. The purpose of this is only to apply the customizations to the
 * base file definition.
 */
public class CSVFileDefinitionModifier {
  CSVFileDefinition _modifyingFileDefinition;
  public CSVFileDefinitionModifier(CSVFileDefinition fileDefinition) {
    _modifyingFileDefinition = fileDefinition;
  }

  /**
   * Applies the modified column definitions to the base definition to create a new file definition that contains everything the customer
   * expects to include in their data files
   * @param baseDefinition The base GW file definition for this type of data. The changes in the _modifyingFileDefinition will be applied to
   *                       the baseDefinition to create a customer file definition.
   */
  public CSVFileDefinition applyTo(CSVFileDefinition baseDefinition){
    CSVFileDefinition baseFileDefCopy = new CSVFileDefinition(baseDefinition.getName(), copyBaseDefColumns(baseDefinition));
    for (CSVFileColumnDef columnDef : _modifyingFileDefinition.getColumnDefs()){
      CSVFileColumnDef baseColumnDef = baseFileDefCopy.getColumnDefForColumnName(columnDef.getName());
      if (baseColumnDef == null)  {
        throw new UnsupportedOperationException("Column " + columnDef.getName() + " is not available ");
      }
      columnDef.applyTo(baseColumnDef);
    }
    return baseFileDefCopy;
  }

  private List<CSVFileColumnDef> copyBaseDefColumns(CSVFileDefinition baseDefinition) {
    List<CSVFileColumnDef> newColumnDefs = new ArrayList<CSVFileColumnDef>(baseDefinition.getColumnDefs().size());
    for (CSVFileColumnDef baseColumnDef : baseDefinition.getColumnDefs()) {
      newColumnDefs.add(new CSVFileColumnDef(baseColumnDef.getName(), baseColumnDef.getType(), baseColumnDef.getFormatString(),
              baseColumnDef.getAlias(), baseColumnDef.getColumnStatus()));
    }
    return newColumnDefs;
  }
}

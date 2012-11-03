package org.apache.solr.util;

public enum PivotListEntry {
  
  FIELD("field", 0),
  
  VALUE("value", 1),
  
  COUNT("count", 2),
  
  PIVOT("pivot", 3),
  
  WEIGHT("weight", 3);
  
  private final String name;
  
  private final int index;
  
  private PivotListEntry(String name, int index) {
    this.name = name;
    this.index = index;
  }
  
  public String getName() {
    return name;
  }
  
  public int getIndex() {
    return index;
  }

}

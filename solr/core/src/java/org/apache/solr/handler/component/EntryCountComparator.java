package org.apache.solr.handler.component;

import java.util.Comparator;
import java.util.Map.Entry;

public class EntryCountComparator implements Comparator<Entry<Object, Integer>> {
  
  @Override
  public int compare(Entry<Object, Integer> o1, Entry<Object, Integer> o2) {
    return (o2.getValue()).compareTo(o1.getValue());
  }

}

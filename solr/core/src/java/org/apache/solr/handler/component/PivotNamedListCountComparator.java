package org.apache.solr.handler.component;

import java.util.Comparator;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;

public class PivotNamedListCountComparator implements Comparator<NamedList<Object>> {
  
  @Override
  public int compare(NamedList<Object> o1, NamedList<Object> o2) {
    Object firstCountObj = o1.get(FacetParams.FACET_SORT_COUNT);
    Object secondCountObj = o2.get(FacetParams.FACET_SORT_COUNT);
    if (firstCountObj instanceof Integer && secondCountObj instanceof Integer) {
      return ((Integer) secondCountObj).compareTo((Integer) firstCountObj);
    } else {
      return 0;
    }
  }

}

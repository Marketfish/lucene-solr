package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;

public class DistributedFacetPivotTest extends BaseDistributedSearchTestCase {
  
  @Override
  public void doTest() throws Exception {
    
    del("*:*");
    index(id, 19, "place_t", "cardiff dublin", "company_t", "microsoft polecat");
    index(id, 20, "place_t", "dublin", "company_t", "polecat microsoft honda");
    index(id, 21, "place_t", "london la dublin", "company_t",
        "microsoft fujitsu honda polecat");
    index(id, 22, "place_t", "krakow london cardiff", "company_t",
        "polecat honda bbc");
    index(id, 23, "place_t", "london", "company_t", "");
    index(id, 24, "place_t", "la", "company_t", "");
    index(id, 25, "place_t", "", "company_t",
        "microsoft polecat honda fujitsu honda bbc");
    index(id, 26, "place_t", "krakow", "company_t", "honda");
    index(id, 27, "place_t", "krakow cardiff dublin london la", "company_t",
        "honda microsoft polecat bbc fujitsu");
    index(id, 28, "place_t", "cork", "company_t",
        "fujitsu rte");
    commit();
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    
    final ModifiableSolrParams params = new ModifiableSolrParams();
    setDistributedParams(params);
    params.add("q", "*");
    params.add("facet", "true");
    params.add("facet.pivot", "place_t,company_t");
    
    QueryResponse rsp = queryServer(params);
    
    List<PivotField> expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    List<PivotField> expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t",
        "microsoft", 2, null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "honda", 2,
        null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "fujitsu",
        1, null));
    List<PivotField> expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "honda", 3,
        null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "bbc", 1,
        null));
    List<PivotField> expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "microsoft",
        2, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "fujitsu",
        2, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "honda", 3,
        null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null));
    List<PivotField> expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots.add(new ComparablePivotField("company_t", "microsoft", 2,
        null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "fujitsu", 2,
        null));
    expectedLAPivots
        .add(new ComparablePivotField("company_t", "honda", 2, null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "bbc", 1, null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "polecat", 2,
        null));
    List<PivotField> expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "polecat",
        2, null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "bbc", 2,
        null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "honda", 3,
        null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "fujitsu",
        1, null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "microsoft",
        1, null));
    List<PivotField> expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCorkPivots.add(new ComparablePivotField("company_t", "fujitsu",
        1, null));
    expectedCorkPivots.add(new ComparablePivotField("company_t", "rte",
        1, null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cardiff", 3,
        expectedCardiffPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "la", 3,
        expectedLAPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "krakow", 3,
        expectedKrakowPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cork", 1,
        expectedCorkPivots));
    
    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    // Useful to check for errors, orders lists and does toString() equality
    // check
//    testOrderedPivotsStringEquality(expectedPlacePivots, placePivots);
    
    assertEquals(expectedPlacePivots, placePivots);
    
    // Test sorting by count
    
    params.set(FacetParams.FACET_SORT, FacetParams.FACET_SORT_COUNT);
    
    rsp = queryServer(params);
    
    placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    testCountSorting(placePivots);
    
    // Test limit
    
    params.set(FacetParams.FACET_LIMIT, 2);
    
    rsp = queryServer(params);
    
    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null));
    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "honda", 3,
        null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat", 3,
        null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots));
    
    placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    assertEquals(expectedPlacePivots, placePivots);
    
    // Test combined limiting method
    
    params.set(FacetParams.FACET_PIVOT_LIMIT_METHOD, FacetParams.COMBINED_PIVOT_FACET_LIMIT);
    
    rsp = queryServer(params);
    
    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "honda",
        3, null));
    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "honda", 3,
        null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat", 3,
        null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots));
    
    placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    assertEquals(expectedPlacePivots, placePivots);
    
    // Test pivot limit field ignore
    
    params.set(FacetParams.FACET_PIVOT_LIMIT_IGNORE, "place_t");
    
    rsp = queryServer(params);
    
    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "honda", 2,
        null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null));
    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "honda", 3,
        null));
    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat",
        3, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "honda", 3,
        null));
    expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots
        .add(new ComparablePivotField("company_t", "honda", 2, null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "polecat", 2,
        null));
    expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "polecat",
        2, null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "honda", 3,
        null));
    expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cardiff", 3,
        expectedCardiffPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "la", 3,
        expectedLAPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "krakow", 3,
        expectedKrakowPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cork", 1,
        expectedCorkPivots));
    
    placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    assertEquals(expectedPlacePivots, placePivots);
  }
  
  // Useful to check for errors, orders lists and does toString() equality check
  private void testOrderedPivotsStringEquality(
      List<PivotField> expectedPlacePivots, List<PivotField> placePivots) {
    Collections.sort(expectedPlacePivots, new PivotFieldComparator());
    for (PivotField expectedPivot : expectedPlacePivots) {
      if (expectedPivot.getPivot() != null) {
        Collections.sort(expectedPivot.getPivot(), new PivotFieldComparator());
      }
    }
    Collections.sort(placePivots, new PivotFieldComparator());
    for (PivotField pivot : placePivots) {
      if (pivot.getPivot() != null) {
        Collections.sort(pivot.getPivot(), new PivotFieldComparator());
      }
    }
    assertEquals(expectedPlacePivots.toString(), placePivots.toString());
  }
  
  private void testCountSorting(List<PivotField> pivots) {
    Integer lastCount = null;
    for (PivotField pivot : pivots) {
      if (lastCount != null) {
        assertTrue(pivot.getCount() <= lastCount);
      }
      lastCount = pivot.getCount();
      if (pivot.getPivot() != null) {
        testCountSorting(pivot.getPivot());
      }
    }
  }
  
  public static class ComparablePivotField extends PivotField {
    
    public ComparablePivotField(String f, Object v, int count,
        List<PivotField> pivot) {
      super(f, v, count, pivot);
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (!obj.getClass().isAssignableFrom(PivotField.class)) return false;
      PivotField other = (PivotField) obj;
      if (getCount() != other.getCount()) return false;
      if (getField() == null) {
        if (other.getField() != null) return false;
      } else if (!getField().equals(other.getField())) return false;
      if (getPivot() == null) {
        if (other.getPivot() != null) return false;
      } else if (!getPivot().equals(other.getPivot())) return false;
      if (getValue() == null) {
        if (other.getValue() != null) return false;
      } else if (!getValue().equals(other.getValue())) return false;
      return true;
    }
  }
  
  public static class UnorderedEqualityArrayList<T> extends ArrayList<T> {
    
    @Override
    public boolean equals(Object o) {
      boolean equal = false;
      if (o instanceof ArrayList) {
        List<?> otherList = (List<?>) o;
        if (size() == otherList.size()) {
          equal = true;
          for (Object objectInOtherList : otherList) {
            if (!contains(objectInOtherList)) {
              equal = false;
            }
          }
        }
      }
      return equal;
    }
    
    public int indexOf(Object o) {
      for (int i = 0; i < size(); i++) {
        if (get(i).equals(o)) {
          return i;
        }
      }
      return -1;
    }
  }
  
  public class PivotFieldComparator implements Comparator<PivotField> {
    
    @Override
    public int compare(PivotField o1, PivotField o2) {
      Integer compare = (Integer.valueOf(o2.getCount())).compareTo(Integer
          .valueOf(o1.getCount()));
      if (compare == 0) {
        compare = ((String) o2.getValue()).compareTo((String) o1.getValue());
      }
      return compare;
    }
    
  }
  
}

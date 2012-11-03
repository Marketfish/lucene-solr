/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.component;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.index.Term;
import org.apache.solr.util.PivotListEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This is thread safe
 * @since solr 4.0
 */
public class PivotFacetHelper
{
  protected Comparator<NamedList<Object>> namedListCountComparator = new PivotNamedListCountComparator();

  /**
   * Designed to be overridden by subclasses that provide different faceting implementations.
   * TODO: Currently this is returning a SimpleFacets object, but those capabilities would
   *       be better as an extracted abstract class or interface.
   */
  @Deprecated
  protected SimpleFacets getFacetImplementation(SolrQueryRequest req, DocSet docs, SolrParams params) {
    return new SimpleFacets(req, docs, params);
  }

  public SimpleOrderedMap<List<NamedList<Object>>> process(ResponseBuilder rb, SolrParams params, String[] pivots) throws IOException {
    if (!rb.doFacets || pivots == null) 
      return null;
    
    int minMatch = params.getInt( FacetParams.FACET_PIVOT_MINCOUNT, 1 );
    
    SimpleOrderedMap<List<NamedList<Object>>> pivotResponse = new SimpleOrderedMap<List<NamedList<Object>>>();
    for (String pivot : pivots) {
      String[] fields = pivot.split(",");  // only support two levels for now
      
      if( fields.length < 2 ) {
        throw new SolrException( ErrorCode.BAD_REQUEST, 
            "Pivot Facet needs at least two fields: "+pivot );
      }

      DocSet docs = rb.getResults().docSet;
      String field = fields[0];
      String subField = fields[1];
      Deque<String> fnames = new LinkedList<String>();
      for( int i=fields.length-1; i>1; i-- ) {
        fnames.push( fields[i] );
      }
      
      SimpleFacets sf = getFacetImplementation(rb.req, rb.getResults().docSet, rb.req.getParams());
      NamedList<Integer> superFacets = sf.getTermCounts(field);
      
      pivotResponse.add(pivot, doPivots(superFacets, field, subField, fnames, rb, docs, minMatch));
    }
    return pivotResponse;
  }
  
  /**
   * Recursive function to do all the pivots
   */
  protected List<NamedList<Object>> doPivots( NamedList<Integer> superFacets, String field, String subField, Deque<String> fnames, ResponseBuilder rb, DocSet docs, int minMatch ) throws IOException
  {
    SolrIndexSearcher searcher = rb.req.getSearcher();
    // TODO: optimize to avoid converting to an external string and then having to convert back to internal below
    SchemaField sfield = searcher.getSchema().getField(field);
    FieldType ftype = sfield.getType();

    String nextField = fnames.poll();

    List<NamedList<Object>> values = new ArrayList<NamedList<Object>>( superFacets.size() );
    for (Map.Entry<String, Integer> kv : superFacets) {
      // Only sub-facet if parent facet has positive count - still may not be any values for the sub-field though
      if (kv.getValue() >= minMatch ) {

        // may be null when using facet.missing
        final String fieldValue = kv.getKey(); 

        // don't reuse the same BytesRef each time since we will be 
        // constructing Term objects used in TermQueries that may be cached.
        BytesRef termval = null;

        SimpleOrderedMap<Object> pivot = new SimpleOrderedMap<Object>();
        pivot.add( "field", field );
        if (null == fieldValue) {
          pivot.add( "value", null );
        } else {
          termval = new BytesRef();
          ftype.readableToIndexed(fieldValue, termval);
          pivot.add( "value", ftype.toObject(sfield, termval) );
        }
        pivot.add( "count", kv.getValue() );
        
        if( subField == null ) {
          values.add( pivot );
        }
        else {
          DocSet subset = null;
          if ( null == termval ) {
            DocSet hasVal = searcher.getDocSet
              (new TermRangeQuery(field, null, null, false, false));
            subset = docs.andNot(hasVal);
          } else {
            Query query = new TermQuery(new Term(field, termval));
            subset = searcher.getDocSet(query, docs);
          }
          SimpleFacets sf = getFacetImplementation(rb.req, subset, rb.req.getParams());
          
          NamedList<Integer> nl = sf.getTermCounts(subField);
          if (nl.size() >= minMatch ) {
            pivot.add( "pivot", doPivots( nl, subField, nextField, fnames, rb, subset, minMatch ) );
            values.add( pivot ); // only add response if there are some counts
          }
        }
      }
    }
    
    // put the field back on the list
    fnames.push( nextField );
    return values;
  }

  private void mergeValueToMap(Map<Object,NamedList<Object>> polecatCounts,
                                        String field, Object value, Integer count,
                                        List<NamedList<Object>> subPivot, int pivotsDone, int numberOfPivots) {
       if (polecatCounts.containsKey(value)) {
           polecatCounts.put(
                   value,
                   mergePivots(polecatCounts.get(value), count, subPivot, pivotsDone,
                           numberOfPivots));
         } else {
           SimpleOrderedMap<Object> pivot = new SimpleOrderedMap<Object>();
           pivot.add(PivotListEntry.FIELD.getName(), field);
           pivot.add(PivotListEntry.VALUE.getName(), value);
           pivot.add(PivotListEntry.COUNT.getName(), count);
           if (subPivot != null) {
               pivot.add(PivotListEntry.PIVOT.getName(),
                       convertPivotsToMaps(subPivot, pivotsDone, numberOfPivots));
             }
           polecatCounts.put(value, pivot);
         }
     }
 
   public Object getFromPivotList(PivotListEntry entryToGet, NamedList<?> pivotList) {
       Object entry = pivotList.get(entryToGet.getName(), entryToGet.getIndex());
       if (entry == null) {
           entry = pivotList.get(entryToGet.getName());
         }
       return entry;
   }
 
   private NamedList<Object> mergePivots(NamedList<Object> existingNamedList,
                                                  Integer countToMerge, List<NamedList<Object>> pivotToMergeList,
                                                  int pivotsDone, int numberOfPivots) {
     if (countToMerge != null) {
         // Cast here, but as we're only putting Integers in above it should be
             // fine
                 existingNamedList.setVal(
                         PivotListEntry.COUNT.getIndex(),
                         ((Integer) getFromPivotList(PivotListEntry.COUNT,
                                 existingNamedList)) + countToMerge);
     }
     if (pivotToMergeList != null) {
         Object existingPivotObj = getFromPivotList(
                 PivotListEntry.PIVOT, existingNamedList);
         if (existingPivotObj instanceof Map) {
             for (NamedList<Object> pivotToMerge : pivotToMergeList) {
                 String nextFieldToMerge = (String) getFromPivotList(
                         PivotListEntry.FIELD, pivotToMerge);
                 Object nextValueToMerge = getFromPivotList(
                         PivotListEntry.VALUE, pivotToMerge);
                 Integer nextCountToMerge = (Integer) getFromPivotList(PivotListEntry.COUNT, pivotToMerge);
                 Object nextPivotToMergeListObj = getFromPivotList(
                         PivotListEntry.PIVOT, pivotToMerge);
                 List nextPivotToMergeList = null;
                 if (nextPivotToMergeListObj instanceof List) {
                     nextPivotToMergeList = (List) nextPivotToMergeListObj;
                   }
                 mergeValueToMap((Map) existingPivotObj, nextFieldToMerge,
                         nextValueToMerge, nextCountToMerge, nextPivotToMergeList,
                         pivotsDone++, numberOfPivots);
               }
           } else {
             existingNamedList.add(
                     PivotListEntry.PIVOT.getName(),
                     convertPivotsToMaps(pivotToMergeList, pivotsDone + 1,
                             numberOfPivots));
           }
     }
     return existingNamedList;
   }
 
   public Map<Object,NamedList<Object>> convertPivotsToMaps(
       List<NamedList<Object>> pivots, int pivotsDone, int numberOfPivots) {
     return convertPivotsToMaps(pivots, pivotsDone, numberOfPivots, null);
   }
 
   public Map<Object,NamedList<Object>> convertPivotsToMaps(
           List<NamedList<Object>> pivots, int pivotsDone, int numberOfPivots,
           Map<Integer,Map<Object,Integer>> fieldCounts) {
     Map<Object,NamedList<Object>> pivotMap = new HashMap<Object,NamedList<Object>>();
     boolean countFields = (fieldCounts != null);
     Map<Object,Integer> thisFieldCountMap = null;
     if (countFields) {
         thisFieldCountMap = getFieldCountMap(fieldCounts, pivotsDone);
       }
     for (NamedList<Object> pivot : pivots) {
         Object valueObj = getFromPivotList(PivotListEntry.VALUE, pivot);
         pivotMap.put(valueObj, pivot);
         if (countFields) {
             Object countObj = getFromPivotList(PivotListEntry.COUNT, pivot);
             int count = 0;
             if (countObj instanceof Integer) {
                 count = (Integer) countObj;
               }
             addFieldCounts(valueObj, count, thisFieldCountMap);
           }
         if (pivotsDone < numberOfPivots) {
             Integer pivotIdx = pivot.indexOf(PivotListEntry.PIVOT.getName(), 0);
             if (pivotIdx > -1) {
                 Object pivotObj = pivot.getVal(pivotIdx);
                 if (pivotObj instanceof List) {
                     pivot.setVal(
                             pivotIdx,
                             convertPivotsToMaps((List) pivotObj, pivotsDone + 1,
                                     numberOfPivots, fieldCounts));
                   }
               }
           }
       }
     return pivotMap;
   }
 
   private List<NamedList<Object>> convertPivotMapToList(
         Map<Object,NamedList<Object>> pivotMap,
          InternalPivotLimitInfo pivotLimitInfo, int currentPivot,
           int numberOfPivots, boolean sortByCount) {
       List<NamedList<Object>> pivots = new ArrayList<NamedList<Object>>();
       currentPivot++;
       List<Object> fieldLimits = null;
       InternalPivotLimitInfo nextPivotLimitInfo = new InternalPivotLimitInfo(
               pivotLimitInfo);
       if (pivotLimitInfo.combinedPivotLimit
               && pivotLimitInfo.fieldLimitsList.size() > 0) {
           fieldLimits = pivotLimitInfo.fieldLimitsList.get(0);
           nextPivotLimitInfo.fieldLimitsList = pivotLimitInfo.fieldLimitsList
                   .subList(1, pivotLimitInfo.fieldLimitsList.size());
         }
       for (Map.Entry<Object,NamedList<Object>> pivot : pivotMap.entrySet()) {
          if (pivotLimitInfo.limit == 0 || !pivotLimitInfo.combinedPivotLimit
                   || fieldLimits == null || fieldLimits.contains(pivot.getKey())) {
               pivots.add(pivot.getValue());
               convertPivotEntryToListType(pivot.getValue(), nextPivotLimitInfo,
                       currentPivot, numberOfPivots, sortByCount);
             }
         }
       if (sortByCount) {
           Collections.sort(pivots, namedListCountComparator);
         }
       if (!pivotLimitInfo.combinedPivotLimit && pivotLimitInfo.limit > 0
               && pivots.size() > pivotLimitInfo.limit) {
           pivots = new ArrayList<NamedList<Object>>(pivots.subList(0,
                   pivotLimitInfo.limit));
         }
       return pivots;
     }
 
   public SimpleOrderedMap<List<NamedList<Object>>> convertPivotMapsToList(
     SimpleOrderedMap<Map<Object,NamedList<Object>>> pivotValues,
     PivotLimitInfo pivotLimitInfo, boolean sortByCount) {
     SimpleOrderedMap<List<NamedList<Object>>> pivotsLists = new SimpleOrderedMap<List<NamedList<Object>>>();
     for (Map.Entry<String,Map<Object,NamedList<Object>>> pivotMapEntry : pivotValues) {
       String pivotName = pivotMapEntry.getKey();
       Integer numberOfPivots = 1 + StringUtils.countMatches(pivotName, ",");
       InternalPivotLimitInfo internalPivotLimitInfo = new InternalPivotLimitInfo(
               pivotLimitInfo, pivotName);
       pivotsLists.add(
               pivotName,
               convertPivotMapToList(pivotMapEntry.getValue(),
                       internalPivotLimitInfo, 0, numberOfPivots, sortByCount));
     }
     return pivotsLists;
   }
 
   private void convertPivotEntryToListType(NamedList<Object> pivotEntry,
                                                   InternalPivotLimitInfo pivotLimitInfo, int pivotsDone,
                                                   int numberOfPivots, boolean sortByCount) {
     if (pivotsDone < numberOfPivots) {
      int pivotIdx = pivotEntry.indexOf(PivotListEntry.PIVOT.getName(), 0);
       if (pivotIdx > -1) {
         Object subPivotObj = pivotEntry.getVal(pivotIdx);
         if (subPivotObj instanceof Map) {
           Map<Object,NamedList<Object>> subPivotMap = (Map) subPivotObj;
           pivotEntry.setVal(
                   pivotIdx,
                   convertPivotMapToList(subPivotMap, pivotLimitInfo, pivotsDone,
                           numberOfPivots, sortByCount));
         }
       }
     }
   }
 
   public Map<Object,Integer> getFieldCountMap(
     Map<Integer,Map<Object,Integer>> fieldCounts, int pivotNumber) {
     Map<Object,Integer> fieldCountMap = fieldCounts.get(pivotNumber);
     if (fieldCountMap == null) {
         fieldCountMap = new HashMap<Object,Integer>();
         fieldCounts.put(pivotNumber, fieldCountMap);
     }
     return fieldCountMap;
   }
 
   public void addFieldCounts(Object name, int count,
                                     Map<Object,Integer> thisFieldCountMap) {
     Integer existingFieldCount = thisFieldCountMap.get(name);
     if (existingFieldCount == null) {
       thisFieldCountMap.put(name, count);
     } else {
       thisFieldCountMap.put(name, existingFieldCount + count);
     }
   }
 
   public static class PivotLimitInfo {
 
     public SimpleOrderedMap<List<List<Object>>> fieldLimitsMap = null;
 
     public int limit = 0;
 
     public boolean combinedPivotLimit = false;
   }
 
   private static class InternalPivotLimitInfo {
 
     public List<List<Object>> fieldLimitsList = null;
 
     public int limit = 0;
 
     public boolean combinedPivotLimit = false;
 
     private InternalPivotLimitInfo() {}
 
     private InternalPivotLimitInfo(PivotLimitInfo pivotLimitInfo,
                                             String pivotName) {
       this.limit = pivotLimitInfo.limit;
       this.combinedPivotLimit = pivotLimitInfo.combinedPivotLimit;
       if (pivotLimitInfo.fieldLimitsMap != null) {
           this.fieldLimitsList = pivotLimitInfo.fieldLimitsMap.get(pivotName);
       }
     }
 
     private InternalPivotLimitInfo(InternalPivotLimitInfo pivotLimitInfo) {
       this.fieldLimitsList = pivotLimitInfo.fieldLimitsList;
       this.limit = pivotLimitInfo.limit;
       this.combinedPivotLimit = pivotLimitInfo.combinedPivotLimit;
     }
   }
}

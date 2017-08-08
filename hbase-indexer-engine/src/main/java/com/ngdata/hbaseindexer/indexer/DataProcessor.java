package com.ngdata.hbaseindexer.indexer;

import org.apache.solr.common.SolrInputDocument;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class DataProcessor {
    List<RowData> dataList = null;
    Map<String , SolrInputDocument> contentData = null;
    Pattern timePattern  = Pattern.compile("(\\d{4}(-)\\d{2}(-)\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s?)");

    DataProcessor(Map<String, SolrInputDocument> data , List<RowData> rowDataList){
        dataList = rowDataList;
        contentData = data;
    }

    public Map <String , SolrInputDocument> dealWithInputData(){
        Map<String, SolrInputDocument> modifiedDataMap  = null ;
        contentData.forEach((k,v) -> {
            String name = k;
            SolrInputDocument document = new SolrInputDocument();
            Collection<String> keyNames = v.getFieldNames();
            keyNames.forEach(fieldName -> {
                String itemValue = v.getFieldValue(fieldName).toString();
                if(timePattern.matcher(itemValue).groupCount() > 0){
                    String formatDateTime = Convert2Solr(itemValue,"yyyy-MM-dd HH:mm:ss");
                    document.addField(fieldName, formatDateTime);
                }else{
                    document.addField(fieldName, v.getFieldValue(fieldName));
                }
            });
            modifiedDataMap.put(name, document);
        });
        return modifiedDataMap;
    }

    public void dealWithRowData(List<RowData> rowDataList) {
    /*
    * This function is waiting for completed, which is used to deal with
    * the original data that is got from hbase and used to add indexer in
    * solr.
    * */
    }

    public static String Convert2Solr(String date, String format) {
        String value = "";
        try {
            DateFormat fmt = new SimpleDateFormat(format);
            Date _date = fmt.parse(date);
            ;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(_date);
            calendar.add(Calendar.HOUR, -8);
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.ROOT);
            value = df.format(calendar.getTime());
        } catch (Exception e) {
        }
        return value;
    }

}
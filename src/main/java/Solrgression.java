import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.*;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: dbraga
 * Date: 4/15/13
 */

public class Solrgression {

  private static final Logger LOG = Logger.getLogger(Solrgression.class);
  private static final String logFile = "REQUEST_LOG_FILE_URI";
  private static final String solrInstanceUriA = "SOLR_URI_A";
  private static final String solrInstanceUriB = "SOLR_URI_B";

  public String httpRequest(GetMethod method){
    String httpResponse = "";
    HttpClient client = new HttpClient();
    try {
      int codeStatus = client.executeMethod(method);
      if (codeStatus == 200){
        // 200 : OK
       InputStreamReader is = new InputStreamReader(method.getResponseBodyAsStream());
       BufferedReader br = new BufferedReader(is);
        String line;
        while((line = br.readLine())!= null) httpResponse += line;
      }

    } catch (IOException e) {
      //TODO: logging
      LOG.fatal(e.getStackTrace().toString());
    } finally {
      method.releaseConnection();
    }
    return httpResponse;
  }

  public  GetMethod prepareGetMethod(String url, NameValuePair[] params){
    GetMethod method = new GetMethod(url);
    method.setQueryString(params);
    return method;
  }

  public JsonNode removeNode(JsonNode jsonNode, String nodeName){
     ObjectNode temp = (ObjectNode) jsonNode;
     temp.remove(nodeName);
     return temp;
  }

  public boolean quickDiff(String one, String two) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode treeA = removeNode(mapper.readTree(one),"responseHeader");
    JsonNode treeB = removeNode(mapper.readTree(two),"responseHeader");
    return !treeA.equals(treeB);
  }

  public void checkForDifferentNodes(JsonNode node1, JsonNode node2){
    Iterator<String> iteratorA =   node1.getFieldNames();
    Iterator<String> iteratorB =   node2.getFieldNames();

    ArrayList<String> nodeNameA = new ArrayList<String>();
    ArrayList<String> nodeNameB = new ArrayList<String>();

    while ( iteratorA.hasNext()){
     nodeNameA.add(iteratorA.next().toString());
    }

    while ( iteratorB.hasNext()){
      nodeNameB.add(iteratorB.next().toString());
    }

    for (String nameA: nodeNameA){
      if (!nodeNameB.contains(nameA)){
        LOG.error(nameA + " node is present in the first server response but it's missing from the second");
        LOG.error(nameA + " content: "+ node1.get(nameA).toString());
      }
    }

    for (String nameB: nodeNameB){
      if (!nodeNameA.contains(nameB)){
        LOG.error(nameB+" node is present in the second server response but it's missing from the first");
        LOG.error(nameB + " content: "+ node2.get(nameB).toString());

      }
    }
  }
  public void checkForDifferentResultsCount(JsonNode node1, JsonNode node2){
    int count1 = Integer.parseInt(node1.get("response").get("numFound").toString());
    int count2 = Integer.parseInt(node2.get("response").get("numFound").toString());
    if (count1!=count2){
      LOG.error("Different number of results returned. "+count1+" vs "+count2);
    }

  }
  public void diff(String one, String two) throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    // We don't need the response header. This will be different in each case
    JsonNode treeA = removeNode(mapper.readTree(one),"responseHeader");
    JsonNode treeB = removeNode(mapper.readTree(two),"responseHeader");
    // Different nodes?
    checkForDifferentNodes(treeA,treeB);
    // Different # of results found?
    checkForDifferentResultsCount(treeA,treeB);

    // Doing a more deep check
    JsonNode docsA = treeA.get("response").get("docs");
    JsonNode docsB = treeB.get("response").get("docs");

    for (int i=0; i< docsA.size(); i++){
      if (!docsA.get(i).toString().equals(docsB.get(i).toString())){

        String docIdA = docsA.get(i).get("id").toString();
        String docIdB = docsB.get(i).get("id").toString();

        if (!docIdA.equals(docIdB)){
        // different docIds, very probable that we're diffing two different documents. Skipping
          LOG.error("Diffing two different documents: DocA["+docIdA+"] vs DocB["+docIdB+"] :");
          //break;
        }

        if (docsA.get(i).size() != docsB.get(i).size()){
        // Different set of fields returned .. different index content. Skipping
          LOG.error("DocA["+docIdA+"]"+" has a different number of fields DocB["+docIdB+"]");
          //break;
        }

        if (docIdA.equals(docIdB)){
          LOG.info("DocA[" + docIdA + "] vs DocB[" + docIdB + "] :");

          JsonNode fieldsA = docsA.get(i);
          JsonNode fieldsB = docsB.get(i);


          Iterator<String> iteratorA =   fieldsA.getFieldNames();
          Iterator<String> iteratorB =   fieldsB.getFieldNames();

          while ( iteratorA.hasNext() && iteratorB.hasNext()  ){
            String fieldNameA = iteratorA.next();
            String valueA = fieldsA.get(fieldNameA).toString();


            String fieldNameB = iteratorB.next();
            String valueB = fieldsB.get(fieldNameB).toString();

            if (!fieldNameA.equals(fieldNameB)){
              LOG.info("\t\t"+"different fieldnames:[ "+fieldNameA+" | "+fieldNameB+ "]");
            } else if (!valueA.equals(valueB)){
              LOG.info("\t\t"+"different values: "+fieldNameA+"["+fieldsA.get(fieldNameA)+" | "+fieldsB.get(fieldNameB)+"]");
            }
          }
        }



    }
    }
  }

  public static class FieldComparator implements Comparator<NameValuePair> {
    @Override
    public int compare(NameValuePair nameValuePair, NameValuePair nameValuePair2) {
      return  nameValuePair.getName().compareTo(nameValuePair2.getName());
    }
  }

  NameValuePair findPairByName(String name, ArrayList<NameValuePair> list){
    for (int i=0; i<list.size(); i++) {
      NameValuePair field = list.get(i);
      if (field.getName() == name){
        return field;
      }
    }
    return null; // no field found; maybe throw an exception
  }

  public ArrayList<NameValuePair> toMap(String json) throws IOException{
    ArrayList<NameValuePair> elements = new ArrayList<NameValuePair>();

     JsonNode jsonNode = new ObjectMapper().readTree(json);
     //LOG.info(jsonNode.get("response").get("docs"));
    // Number of docs
    //elements.add(new NameValuePair("numFound",jsonNode.get("response").get("numFound").toString()));

    // All fields + values
     for (JsonNode doc: jsonNode.get("response").get("docs")){
       Iterator<String> iterator =   doc.getFieldNames();
       while ( iterator.hasNext() ){
         String fieldName = iterator.next();
         elements.add(new NameValuePair(fieldName,doc.get(fieldName).toString()));
       }
     }
    Collections.sort(elements,new FieldComparator());
    return elements;
  }



  //Read more: http://javarevisited.blogspot.com/2013/02/how-to-convert-json-string-to-java-object-jackson-example-tutorial.html#ixzz2QeFSJbwH

  public String[] getJsonSolrResponses(NameValuePair[] sharedConfig, String firstSolrInstanceUri, String secondSolrInstanceUri){
    String[] response = new String[2];

    response[0] = httpRequest(prepareGetMethod(
            firstSolrInstanceUri,
            sharedConfig
    ));
    response[1] = httpRequest(prepareGetMethod(
            secondSolrInstanceUri,
            sharedConfig
    ));

    return response;
  }


  public NameValuePair[] parseLogLine(String line) throws UnsupportedEncodingException {

    Pattern pattern = Pattern.compile("select(/)?\\S*");
    Matcher matcher = pattern.matcher(line);
    String fixedLine="";

    if (matcher.find()) {
      ArrayList<NameValuePair> params = new ArrayList<NameValuePair>();


      int offSet = (matcher.group(0).contains("select/"))? 8 : 7;
      fixedLine = matcher.group(0).substring(matcher.group(0).indexOf("select")+offSet,matcher.group(0).length());
      String[] splitted = fixedLine.split("&");



      for (String elements: splitted){
        String decodedElement = URLDecoder.decode(elements,"UTF-8");
        String key = decodedElement.split("=")[0];
        // Ignore the wt param, force wt=json after
        if (!key.equals("wt")){
          String value = decodedElement.split("=")[1];
          params.add(new NameValuePair(key,value));
        }
      }

      // Add wt=json
      params.add(new NameValuePair("wt","json"));

      return params.toArray(new NameValuePair[params.size()]);


    } else {
      LOG.warn("Skipped: "+line);
      return  null;
    }




  }

  public String printParams(NameValuePair[] nameValuePairs){
    String params ="";
    for (NameValuePair param: nameValuePairs){
      params += "<"+param.getName()+","+param.getValue()+">";
    }
    return params;
  }

  public int count(String filename) throws IOException {
    InputStream is = new BufferedInputStream(new FileInputStream(filename));
    try {
      byte[] c = new byte[1024];
      int count = 0;
      int readChars = 0;
      boolean empty = true;
      while ((readChars = is.read(c)) != -1) {
        empty = false;
        for (int i = 0; i < readChars; ++i) {
          if (c[i] == '\n') {
            ++count;
          }
        }
      }
      return (count == 0 && !empty) ? 1 : count;
    } finally {
      is.close();
    }
  }

  public void run() throws Exception {

    int totalDiffDocuments = 10;
    int batchSize = 3;

    int linesToBeProcessed = count(logFile);


    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(logFile)));
    try {
      int pointer = 1;
      String line;
      while ((line = br.readLine()) != null) {

        LOG.info("Processing "+pointer+"/"+linesToBeProcessed);
        // process line
        if (line.contains("GET") && !line.contains("rentals_buffer") && !line.contains("admin")){
          // GET HTTP method
          NameValuePair[] nvp = parseLogLine(line);


          if (nvp!=null){
            LOG.info("Line: "+line);
            LOG.info("Parsed: "+printParams(nvp));
            String[] jsonResponse = getJsonSolrResponses(nvp, solrInstanceUriA, solrInstanceUriB);
            if (jsonResponse[0]!="" && jsonResponse[1]!="") {
              if (!quickDiff(jsonResponse[0], jsonResponse[1])){
                LOG.info("no diff found \u2713");
              } else {
                LOG.info("############### Found diffs ##############");
                diff(jsonResponse[0], jsonResponse[1]);
                LOG.info("##########################################");
              }
            } else LOG.error("No valid json response. This line is not valid: "+line);
          }
        }
        pointer++;
      }
    } finally {
      br.close();
    }
  }



  public static void main(String[] args) throws Exception {
      new Solrgression().run();
  }
}

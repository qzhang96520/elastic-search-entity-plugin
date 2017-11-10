Our plugin will be loaded into Elasticsearch runtime before the server starts. The nature of my plugin is action plugin, meaning it extends Elasticsearch’s runtime action by adding a customized RESTful endpoint.

When creating a new endpoint you have to extend the class org.elasticsearch.rest.BaseRestHandler. But before going there, we first inilialize it in our plugin. To do that we implement the interface org.elasticsearch.plugins.ActionPlugin and implement the method getRestHandlers. 

```

   @Override

 public List<RestHandler> getRestHandlers(Settings settings, RestController restController,


   ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,


   IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {


 return Arrays.asList(


     new ClusteringAction.RestClusteringAction(settings, restController));


 }
```
This endpoint provides an action class called RestClusteringAction.
Next is implementing the RestClusteringAction class. Below the first part, the constructor and the method that handles the request. In the constructor we define the endpoint url patterns that this endpoint supports. This class extends org.elasticsearch.rest.BaseRestHandler as you can see in my code below.

```
public static class RestClusteringAction extends BaseRestHandler 
```


The architecture diagram of my plugin is drawn below.
![draw](https://lh3.googleusercontent.com/PRipipbKMMGAvJBUmzNbdw9nUSXGt1Mpobf6W4G6d1aeJTMu67NJQgxpd3pkb-GIP0P7raYG12FHGDqRvfOj7JoqPaEIZtgF-N4HhVdPfc8Uaah7XeLOXp2efAuy-kafinoGZ9eF)


The plugin contains a restful handler which has customized response handler and request handler. By modifying response as well as request, we can achieve our entity search business logic.


Request handler will parse the request and create a new request that is supported by Elasticsearch.

```
Map<String, Object> asMap = parser.mapOrdered();
               Map<String, Object> searchRequestMap = (Map<String, Object>) asMap.get("search_request");
               final String DELIMITER = " ";
               if (searchRequestMap != null) {
                   if (this.searchRequest == null) {
                       searchRequest = new SearchRequest();
                   }
                   String[] tokens = ((String)searchRequestMap.get("query")).split(DELIMITER);
                   searchRequestMap.put("query", new HashMap<String, Object>());
                   HashMap<String, Object> query = (HashMap<String, Object>)searchRequestMap.get("query");
                   query.put("span_near", new HashMap<String, Object>());
                   List<HashMap<String, Object>> hashMapArray = new ArrayList<>();
                   int i = 0;
                   for(String token:tokens) {
                       if(token.charAt(0)=='#') {
                           token = token.substring(1);
                           HashMap<String, Object> hashMapElement = new HashMap<String, Object> ();
                           hashMapElement.put("field_masking_span", new HashMap<String, Object>());
                           HashMap<String, Object> fieldMaskingSpan = (HashMap<String, Object>) hashMapElement.get("field_masking_span");
                           fieldMaskingSpan.put("query", new HashMap<String, Object>());
                           HashMap<String, Object> queryH = (HashMap<String, Object>) fieldMaskingSpan.get("query");
                           queryH.put("span_multi", new HashMap<String, Object>());
                           HashMap<String, Object> spanMulti = (HashMap<String, Object>) queryH.get("span_multi");
                           spanMulti.put("match", new HashMap<String, Object>());
                           HashMap<String, Object> match = (HashMap<String, Object>) spanMulti.get("match");
                           match.put("wildcard", new HashMap<String, Object>());
                           HashMap<String, Object> wildcard = (HashMap<String, Object>) match.get("wildcard");
                           wildcard.put(token, "*");
                           fieldMaskingSpan.put("field", "text");
                           hashMapArray.add(hashMapElement);                           
                       }
                       else {
                           HashMap<String, Object> hashMapElement = new HashMap<String, Object> ();
                           hashMapElement.put("span_term", new HashMap<String, Object>());
                           HashMap<String, Object> spanTerm = (HashMap<String, Object>) hashMapElement.get("span_term");
                           spanTerm.put("text", token);
                           hashMapArray.add(hashMapElement);
                       }
                   }
                   ((HashMap<String, Object>) query.get("span_near")).put("clauses", hashMapArray);
                   ((HashMap<String, Object>) query.get("span_near")).put("slop", 100);
                   System.out.println(searchRequestMap);
                   XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).map(searchRequestMap);
                   XContentParser searchXParser = XContentFactory.xContent(XContentType.JSON)
                           .createParser(xContentRegistry, builder.bytes());
                   QueryParseContext parseContext = new QueryParseContext(searchXParser);
                   SearchSourceBuilder searchSourceBuilder =
                           SearchSourceBuilder.fromXContent(parseContext);
                   searchRequest.source(searchSourceBuilder);
               }
           } catch (Exception e) {
               String sSource = "_na_";
               try {
                   sSource = XContentHelper.convertToJson(source, false, false, XContentFactory.xContentType(source));
               } catch (Throwable e1) {
                   // ignore
               }
               e.printStackTrace();
               throw new ClusteringException("Failed to parse source [" + sSource + "]" + e, e);
           }
       }
```

The code above will handle that logic. The request it takes will be a HashMap structure, and what we do is to find the value with key “query”, and translate the query and replace the value.


The other component is the response handler. Currently we are appending grouped and ranked result. We will group the results by it entity content.  

```
if (searchResponse != null) {
               searchResponse.innerToXContent(builder, ToXContent.EMPTY_PARAMS);
           }   

           SearchHit[] hits = searchResponse.getHits().getHits();
           HashMap<String, List<SearchHit>> hm = new HashMap<>();

           for(SearchHit hit : hits) {
               String entityContent = (String)hit.sourceAsMap().get("entityContent");
               List<SearchHit> ls = hm.getOrDefault(entityContent, new ArrayList<SearchHit> ());
               ls.add(hit);
               hm.put(entityContent, ls);
           }

           List<List<String>> list = new ArrayList<>();
           for(String name : hm.keySet()) {
               List<String> cur = new ArrayList<>();
               List<SearchHit> hl = hm.get(name);
               cur.add(name);
               for(SearchHit h: hl) 
                   cur.add(String.valueOf(h.getId()));
               list.add(cur);     
           }

           Collections.sort(list, new Comparator<List<String>>(){
               public int compare(List<String> a, List<String> b) {
                   return b.size() - a.size();
               }
           });

           builder.startArray(Fields.CLUSTERS);

           for(List<String> ls : list) {
               builder.startObject();
               builder.field("name", ls.get(0));
               builder.startArray("document");
               for(int i = 1; i < ls.size(); i++ ){
                   builder.startObject();
                   builder.field("id", ls.get(i));
                   builder.endObject();
               }
               builder.endArray();
               builder.endObject();
           }

           builder.endArray();
           return builder;
```
ClusteringPlugin.java: This file will be loaded when we start ElasticSearch. In this class, we basically wrote the getter method of our restful handlers. 

ClusteringAction.java: In this file we implemented the restful handler. There are several sub-classes in this class. I will describe them below.

RestClusteringAction: In RestClusteringAction class, we define the API name as _search_with_clusters. We also have a method called prepareRequest, which is pretty self-explantory. In this method, we define the action of parsing and call-back.
ClusteringActionResponse: This class is used as call-back data strcuture. It will group and rank the results, and convert them to json format. 

ClusteringActionRequestBuilder: This class provides the builder for clustering action request. The process of converting EntitySearch format to ElasticSearch format happens here. The `source` method in this class will read the data from user input as Java's hashMap data structure, and convert it to the spanning query the ElasticSearch supports.


Let me explain the workflow ot the plugin.


When Elasticsearch receives the request from clients, it will decide it action by looking at the request’s route. In our case, the route is “search_with_clusters”.
The action handler for this route will have a prepareRequest method that handles the request and sends the prepared request to other services. During this process, our translation happens and we also attach a call-back method after we get the response from other services. This call-back method will handle the json-generation process and ranking.
After the request has been sent to other services, there is nothing that our plugin needs to do.

To compile my code, we will want to run the command ./gradlew clean assemble. Currently I am using gradle to build the java software. Gradlew is the file provided by gradle. ‘Clean assemble’ means that we don’t want to run tests, which we don’t have. After running this command, we will get a zip file in path.of.plugin/build/distribution/. To use this zip file, we can find a binary file in Elasticsearch, which is called elasticsearch-plugin. To install our compiled plugin, you want to run ./elasticsearch-plugin install file:///{you-path-to-the-zip-file}.


Please notice that this plugin needs to have the version of Elasticsearch specified and if you will not be able to install the plugin that has different version of Elasticsearch. To change the version, you can edit build.gradle and find the two variables that are called version_es and version to specify the version.

```
version = '5.6.1'

group = 'org.entitysearch'


buildscript {

    ext {

        // ES version we depend upon/ compile against.

        version_es = '5.6.1'

    }


    repositories {

        mavenLocal()

        mavenCentral()

        jcenter()

    }


    dependencies {

        classpath "org.elasticsearch.gradle:build-tools:" + version_es

    }

}
```
or example, currently the version is 5.6.1. You can change these two variables to change the version and gradle will search for the newer version in its own repository.


There are also some unnecessary codes that exist in my plugin. `ClusteringException`, Preconditions.java are these codes. We can avoid using them by refactoring. However, we may want these functions later.


For entity semantic document,

```
if(searchRequestMap.get("type")!=null && ((String)searchRequestMap.get("type")).equals("d_document")) {

    flag = true;

    searchRequestMap.remove("type");

}
```
If we find the document that is d_document, meaning it is entity semantic document, we will set the flag to be true. And later in the translation part,

```
if(flag)

                                spanTerm.put(token + "_begin", "oentityo");   
```

We are adding “_begin” to each spanTerm because there could entities that consist of more than one words so we need to have “begin” and “end” in order to tag all these words. It will work the same as entity document after that.
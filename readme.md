# Elastic java client

Wrapper around elastic rest client with support for Akka streams, javaslang et JsonLibJavasLang. 

## Installation 

### Repository :

```groovy
repositories {
    mavenCentral()
    maven {
        url 'https://raw.githubusercontent.com/larousso/elastic-java-rest-client/master/repository/releases/'
    }
    flatDir {
        dirs 'libs'
    }
}
```

### Dependency :

```groovy
dependencies {
    compile("com.adelegue:elastic-java-rest-client:1.0.13")
}
```

## Usage
 
 
```java 

//Create a client 
Elastic elasticClient = new Elastic(new HttpHost("localhost", port));
Elastic elasticClient = new Elastic(List.of(new HttpHost("localhost", port)), Option.some("user"), Option.some("password"));
 
//Create an index 
CompletionStage<JsValue> indexCreationResponse = elasticClient.createIndex("monIndex", Json.obj());
 
//Create mapping 
CompletionStage<JsValue> mappingCreation = elasticClient.createMapping("monIndex", "monType", Json.obj(
        $("properties",
                $("name", Json.obj(
                        $("type", "string"),
                        $("index", "not_analyzed")
                ))
        )
));

//Index a document 
CompletionStage<IndexResponse> indexResult = elasticClient.index(INDEX, TYPE, Json.obj($("name", "Jean Claude Dus")), Option.some("1"));

//Search 
CompletionStage<SearchResponse> search = elasticClient.search(INDEX, TYPE, Json.obj($("query", $("match_all", Json.obj()))));
//and then convert data 
CompletionStage<List<Person>> persons = search.thenApply( resp -> searchResponse.hits.hitsAs(Person.read));


//Bulk with Akka streams
java.util.List<BulkResponse> response = Source
        .range(1, 500)
        .map(i -> BulkItem.create(INDEX, TYPE, String.valueOf(i), Json.obj().with("name", "name-" + i)))
        .via(elasticClient.bulk(5, 2))
        .runWith(Sink.seq(), ActorMaterializer.create(system))
        .toCompletableFuture()
        .get();


 ```
package elastic;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import akka.testkit.SocketUtil;
import elastic.request.BulkItem;
import elastic.response.BulkResponse;
import elastic.response.GetResponse;
import elastic.response.IndexResponse;
import javaslang.control.Option;
import org.apache.http.HttpHost;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by adelegue on 28/10/2016.
 */
public class ElasticTest {

    private static final String INDEX = "test";
    private static final String TYPE = "test";

    private static Elastic elasticClient;

    private static NodeStarter nodeStarter;

    static ActorSystem system = ActorSystem.create();

    @BeforeClass
    public static void init() {
        Integer port = SocketUtil.temporaryServerAddress("localhost", false).getPort();
        nodeStarter = new NodeStarter(port);
        elasticClient = new Elastic(new HttpHost("localhost", port));
    }

    @Before
    public void cleanUpIndices() {
        try {
            elasticClient.deleteIndex(INDEX).toCompletableFuture().get();
        } catch (Exception e) {}
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        elasticClient.close();
        nodeStarter.closeNode();
        JavaTestKit.shutdownActorSystem(system);
    }

    @Test
    public void index_creation_should_work() throws ExecutionException, InterruptedException {

        Boolean exists = elasticClient.indexExists(INDEX).toCompletableFuture().get();
        Assertions.assertThat(exists).isFalse();
        createIndexWithMapping();
        Assertions.assertThat(elasticClient.indexExists(INDEX).toCompletableFuture().get()).isTrue();
        JsValue index = elasticClient.getIndex(INDEX).toCompletableFuture().get();
        String type = index.asObject()
                .field("test").asObject()
                .field("mappings").asObject()
                .field("test").asObject()
                .field("properties").asObject()
                .field("name").asObject()
                .field("type").asString();
        Assertions.assertThat(type).isEqualTo("string");
    }



    @Test
    public void mapping_creation_should_work() throws ExecutionException, InterruptedException {
        Boolean exists = elasticClient.indexExists(INDEX).toCompletableFuture().get();
        Assertions.assertThat(exists).isFalse();
        elasticClient.createIndex(INDEX, Json.obj()).toCompletableFuture().get();

        Assertions.assertThat(elasticClient.indexExists(INDEX).toCompletableFuture().get()).isTrue();
        elasticClient.createMapping(INDEX, "test", Json.obj()
                .with("properties", Json.obj()
                        .with("name", Json.obj()
                                .with("type", "string")
                                .with("index", "not_analyzed")
                        )
                )).toCompletableFuture().get();

        JsValue index = elasticClient.getIndex(INDEX).toCompletableFuture().get();
        String type = index.asObject()
                .field("test").asObject()
                .field("mappings").asObject()
                .field("test").asObject()
                .field("properties").asObject()
                .field("name").asObject()
                .field("type").asString();
        Assertions.assertThat(type).isEqualTo("string");
    }

    @Test
    public void index_data_should_work() throws ExecutionException, InterruptedException {
        createIndexWithMapping();
        IndexResponse indexResponse = this.elasticClient.index(INDEX, TYPE, Json.obj().with("name", "Jean Claude Dus"), Option.some("1")).toCompletableFuture().get();

        Assertions.assertThat(indexResponse.created).isTrue();
        Assertions.assertThat(indexResponse._id).isEqualTo("1");

        GetResponse elt = this.elasticClient.get(INDEX, TYPE, "1").toCompletableFuture().get();
        Assertions.assertThat(elt.found).isTrue();
        Option<Person> mayBePerson = elt.as(Person.read);
        ElasticTest.Person person = mayBePerson.get();
        Assertions.assertThat(person.name).isEqualTo("Jean Claude Dus");
    }

    @Test
    public void bulk_indexing_should_work() throws ExecutionException, InterruptedException {
        createIndexWithMapping();
        java.util.List<BulkResponse> response = Source
                .range(1, 500)
                .map(i -> BulkItem.create(INDEX, TYPE, String.valueOf(i), Json.obj().with("name", "name-" + i)))
                .via(this.elasticClient.bulk(5, 2))
                .runWith(Sink.seq(), ActorMaterializer.create(system))
                .toCompletableFuture()
                .get();

        Assertions.assertThat(response).hasSize(100);
        this.elasticClient.refresh(INDEX).toCompletableFuture().get();

        Long count = this.elasticClient.count(INDEX).toCompletableFuture().get();
        Assertions.assertThat(count).isEqualTo(500);
    }

    @Test
    public void bulk_indexing_with_errors() throws ExecutionException, InterruptedException {
        createIndexWithMapping();
        java.util.List<BulkResponse> responses = Source.range(1, 10).concat(Source.range(1, 10))
                .map(i -> BulkItem.create(INDEX, TYPE, String.valueOf(i), Json.obj().with("name", "name-" + i)))
                .via(this.elasticClient.bulk(20, 1, TimeUnit.SECONDS, 2))
                .runWith(Sink.seq(), ActorMaterializer.create(system))
                .toCompletableFuture()
                .get();

        Assertions.assertThat(responses).hasSize(1);
        BulkResponse response = responses.get(0);
        Assertions.assertThat(response.errors).isTrue();
        Assertions.assertThat(response.getErrors()).hasSize(10);

        this.elasticClient.refresh(INDEX).toCompletableFuture().get();

        Long count = this.elasticClient.count(INDEX).toCompletableFuture().get();
        Assertions.assertThat(count).isEqualTo(10);
    }

    private void createIndexWithMapping() throws ExecutionException, InterruptedException {
        elasticClient.createIndex(INDEX, Json.obj()
                .with("mappings", Json.obj()
                        .with("test", Json.obj()
                                .with("properties", Json.obj()
                                        .with("name", Json.obj()
                                                .with("type", "string")
                                                .with("index", "not_analyzed")
                                        )
                                )
                        )
                )
        )
                .toCompletableFuture().get();
    }

    public static class Person {
        public static final Reader<ElasticTest.Person> read = Json.reads(ElasticTest.Person.class);
        public String name;
    }

}
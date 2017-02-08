package elastic;

import akka.actor.ActorSystem;
import akka.japi.pf.FI;
import akka.japi.pf.Match;
import akka.japi.pf.PFBuilder;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import akka.testkit.SocketUtil;
import elastic.request.BulkItem;
import elastic.response.BulkResponse;
import elastic.response.GetResponse;
import elastic.response.IndexResponse;
import javaslang.control.Either;
import javaslang.control.Option;
import org.apache.http.HttpHost;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;


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
        assertThat(exists).isFalse();
        createIndexWithMapping();
        assertThat(elasticClient.indexExists(INDEX).toCompletableFuture().get()).isTrue();
        JsValue index = elasticClient.getIndex(INDEX).toCompletableFuture().get();
        String type = index.asObject()
                .field("test").asObject()
                .field("mappings").asObject()
                .field("test").asObject()
                .field("properties").asObject()
                .field("name").asObject()
                .field("type").asString();
        assertThat(type).isEqualTo("string");
    }



    @Test
    public void mapping_creation_should_work() throws ExecutionException, InterruptedException {
        Boolean exists = elasticClient.indexExists(INDEX).toCompletableFuture().get();
        assertThat(exists).isFalse();
        elasticClient.createIndex(INDEX, Json.obj()).toCompletableFuture().get();

        assertThat(elasticClient.indexExists(INDEX).toCompletableFuture().get()).isTrue();
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
        assertThat(type).isEqualTo("string");
    }

    @Test
    public void index_data_should_work() throws ExecutionException, InterruptedException {
        createIndexWithMapping();
        IndexResponse indexResponse = elasticClient.index(INDEX, TYPE, Json.obj().with("name", "Jean Claude Dus"), Option.some("1")).toCompletableFuture().get();

        assertThat(indexResponse.created).isTrue();
        assertThat(indexResponse._id).isEqualTo("1");

        GetResponse elt = elasticClient.get(INDEX, TYPE, "1").toCompletableFuture().get();
        assertThat(elt.found).isTrue();
        Option<Person> mayBePerson = elt.as(Person.read);
        ElasticTest.Person person = mayBePerson.get();
        assertThat(person.name).isEqualTo("Jean Claude Dus");
    }

    @Test
    public void bulk_indexing_should_work() throws ExecutionException, InterruptedException {
        createIndexWithMapping();
        java.util.List<BulkResponse> response = Source
                .range(1, 500)
                .map(i -> BulkItem.create(INDEX, TYPE, String.valueOf(i), Json.obj().with("name", "name-" + i)))
                .via(elasticClient.bulk(5, 2))
                .runWith(Sink.seq(), ActorMaterializer.create(system))
                .toCompletableFuture()
                .get();

        assertThat(response).hasSize(100);
        elasticClient.refresh(INDEX).toCompletableFuture().get();

        Long count = elasticClient.count(INDEX).toCompletableFuture().get();
        assertThat(count).isEqualTo(500);
    }

    @Test
    public void bulk_indexing_with_errors() throws ExecutionException, InterruptedException {
        createIndexWithMapping();
        java.util.List<BulkResponse> responses = Source.range(1, 10).concat(Source.range(1, 10))
                .map(i -> BulkItem.create(INDEX, TYPE, String.valueOf(i), Json.obj().with("name", "name-" + i)))
                .via(elasticClient.bulk(20, FiniteDuration.create(1, TimeUnit.SECONDS), 2))
                .runWith(Sink.seq(), ActorMaterializer.create(system))
                .toCompletableFuture()
                .get();

        assertThat(responses).hasSize(1);
        BulkResponse response = responses.get(0);
        assertThat(response.errors).isTrue();
        assertThat(response.getErrors()).hasSize(10);

        elasticClient.refresh(INDEX).toCompletableFuture().get();

        Long count = elasticClient.count(INDEX).toCompletableFuture().get();
        assertThat(count).isEqualTo(10);
    }


    @Test
    public void bulk_indexing_with_retry_should_work() throws ExecutionException, InterruptedException {
        createIndexWithMapping();
        java.util.List<Either<Elastic.BulkFailure, BulkResponse>> response = Source
                .range(1, 500)
                .map(i -> BulkItem.create(INDEX, TYPE, String.valueOf(i), Json.obj().with("name", "name-" + i)))
                .via(elasticClient.bulkWithRetry(5, 2, 2, FiniteDuration.create(1, TimeUnit.SECONDS), Elastic.RetryMode.LineareLatency))
                .runWith(Sink.seq(), ActorMaterializer.create(system))
                .toCompletableFuture()
                .get();

        assertThat(response).hasSize(100);
        elasticClient.refresh(INDEX).toCompletableFuture().get();

        Long count = elasticClient.count(INDEX).toCompletableFuture().get();
        assertThat(count).isEqualTo(500);
    }

    @Test
    public void bulk_indexing_with_retry_with_errors() throws ExecutionException, InterruptedException {
        createIndexWithMapping();
        Long start = System.currentTimeMillis();


        List<Either<Elastic.BulkFailure, BulkResponse>> results = Source.range(1, 10).concat(Source.range(1, 10))
                .map(i -> BulkItem.create(INDEX, TYPE, String.valueOf(i), Json.obj().with("name", "name-" + i)))
                .via(elasticClient.bulkWithRetry(
                        5,
                        1,
                        2,
                        FiniteDuration.create(1, TimeUnit.SECONDS),
                        Elastic.RetryMode.ExponentialLatency,
                        pair -> pair._1.isFailure() || (pair._1.isSuccess() && pair._1.get().errors)
                ))
                .runWith(Sink.seq(), ActorMaterializer.create(system))
                .toCompletableFuture()
                .get();

        Long stop = System.currentTimeMillis();

        Long length = 1000L * 6;

        assertThat(stop - start).isGreaterThan(length);

        assertThat(results).hasSize(4);

        javaslang.collection.List<Elastic.BulkFailure> onError = javaslang.collection.List.ofAll(results).filter(Either::isLeft).map(Either::getLeft);
        assertThat(onError).hasSize(2);
        onError.forEach(err ->
                assertThat(err.bulkResponse.getErrors()).hasSize(5)
        );

        elasticClient.refresh(INDEX).toCompletableFuture().get();

        Long count = elasticClient.count(INDEX).toCompletableFuture().get();
        assertThat(count).isEqualTo(10);
    }

    @Test
    public void create_exists_get_delete_template() throws ExecutionException, InterruptedException {
        Boolean tplExists = elasticClient.templateExists("test").toCompletableFuture().get();
        assertThat(tplExists).isFalse();
        String template = "{" +
                "    \"template\" : \"te*\", " +
                "    \"settings\" : { " +
                "        \"number_of_shards\" : 1 " +
                "    }, " +
                "    \"aliases\" : { " +
                "        \"alias1\" : {}, " +
                "        \"alias2\" : { " +
                "            \"filter\" : { " +
                "                \"term\" : {\"user\" : \"kimchy\" } " +
                "            }, " +
                "            \"routing\" : \"kimchy\" " +
                "        }, " +
                "        \"{index}-alias\" : {}  " +
                "    } " +
                "} ";
        JsValue expectedTemplate = Json.parse(template);
        elasticClient.createTemplate("test", expectedTemplate).toCompletableFuture().get();
        tplExists = elasticClient.templateExists("test").toCompletableFuture().get();
        assertThat(tplExists).isTrue();
        JsValue jsTemplate = elasticClient.getTemplate("test").toCompletableFuture().get();
        assertThat(jsTemplate.exists("test")).isTrue();
        elasticClient.deleteTemplate("test").toCompletableFuture().get();
        tplExists = elasticClient.templateExists("test").toCompletableFuture().get();
        assertThat(tplExists).isFalse();
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

    public static class RecoverBuilder {
        private RecoverBuilder() {
        }

        /**
         * Return a new {@link PFBuilder} with a case statement added.
         *
         * @param type   a type to match the argument against
         * @param apply  an action to apply to the argument if the type matches
         * @return       a builder with the case statement added
         */
        public static <P extends Throwable, T> PFBuilder<Throwable, T> match(final Class<P> type, FI.Apply<P, T> apply) {
            return Match.match(type, apply);
        }

        /**
         * Return a new {@link PFBuilder} with a case statement added.
         *
         * @param type       a type to match the argument against
         * @param predicate  a predicate that will be evaluated on the argument if the type matches
         * @param apply      an action to apply to the argument if the type matches and the predicate returns true
         * @return           a builder with the case statement added
         */
        public static <P extends Throwable, T> PFBuilder<Throwable, T> match(final Class<P> type,
                                                                                                     FI.TypedPredicate<P> predicate,
                                                                                                     FI.Apply<P, T> apply) {
            return Match.match(type, predicate, apply);
        }

        /**
         * Return a new {@link PFBuilder} with a case statement added.
         *
         * @param apply      an action to apply to the argument
         * @return           a builder with the case statement added
         */
        public static <T> PFBuilder<Throwable, T> matchAny(FI.Apply<Throwable, T> apply) {
            return Match.matchAny(apply);
        }
    }


}
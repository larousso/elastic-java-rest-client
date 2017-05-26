package elastic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.reactivecouchbase.json.Syntax.$;

import java.io.IOException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;

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
import elastic.response.SearchResponse;
import javaslang.collection.HashMap;
import javaslang.collection.List;
import javaslang.collection.Map;
import javaslang.control.Either;
import javaslang.control.Option;
import scala.concurrent.duration.FiniteDuration;

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
			elasticClient.deleteIndex(INDEX+"*").toCompletableFuture().get();
		} catch (Exception e) {
		}
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
		String type = index.asObject().field("test").asObject().field("mappings").asObject().field("test").asObject().field("properties").asObject()
				.field("name").asObject().field("type").asString();
		assertThat(type).isIn("string", "keyword");
	}


	@Test
	public void mapping_creation_should_work() throws ExecutionException, InterruptedException {
		Boolean exists = elasticClient.indexExists(INDEX).toCompletableFuture().get();
		assertThat(exists).isFalse();
		Boolean mappingExists = elasticClient.mappingExists(INDEX, TYPE).toCompletableFuture().get();
		assertThat(mappingExists).isFalse();
		CompletionStage<JsValue> indexCreationResponse = elasticClient.createIndex(INDEX, Json.obj());
		indexCreationResponse.toCompletableFuture().get();

		assertThat(elasticClient.indexExists(INDEX).toCompletableFuture().get()).isTrue();
		CompletionStage<JsValue> mappingCreation = elasticClient.createMapping(INDEX, TYPE,
				Json.obj($("properties", $("name", Json.obj($("type", "string"), $("index", "not_analyzed"))))));
		mappingCreation.toCompletableFuture().get();

        assertThat(elasticClient.mappingExists(INDEX, TYPE).toCompletableFuture().get()).isTrue();

		JsValue index = elasticClient.getIndex(INDEX).toCompletableFuture().get();
		String type = index
				.field(TYPE)
				.field("mappings")
				.field("test")
				.field("properties")
				.field("name").field("type").asString();
		assertThat(type).isIn("string", "keyword");

		JsValue mapping = elasticClient.getMapping(INDEX, TYPE).toCompletableFuture().get();

		assertThat(mapping.field(INDEX).field("mappings")).isEqualTo(index.field(INDEX).field("mappings"));
	}

	@Test
	public void index_data_should_work() throws ExecutionException, InterruptedException {
		createIndexWithMapping();
		CompletionStage<IndexResponse> indexResult = elasticClient.index(INDEX, TYPE, Json.obj($("name", "Jean Claude Dus")), Option.some("1"));
		IndexResponse indexResponse = indexResult.toCompletableFuture().get();

		assertThat(indexResponse.created).isTrue();
		assertThat(indexResponse._id).isEqualTo("1");

		GetResponse elt = elasticClient.get(INDEX, TYPE, "1").toCompletableFuture().get();
		assertThat(elt.found).isTrue();
		Option<Person> mayBePerson = elt.as(Person.read);
		ElasticTest.Person person = mayBePerson.get();
		assertThat(person.name).isEqualTo("Jean Claude Dus");
	}

	@Test
	public void search_data_should_work() throws ExecutionException, InterruptedException {
		createIndexWithMapping();
		IndexResponse indexResponse = elasticClient.index(INDEX, TYPE, Json.obj($("name", "Jean Claude Dus")), Option.some("1")).toCompletableFuture()
				.get();

		elasticClient.refresh().toCompletableFuture().get();

		assertThat(indexResponse.created).isTrue();
		assertThat(indexResponse._id).isEqualTo("1");

		CompletionStage<SearchResponse> search = elasticClient.search(INDEX, TYPE, Json.obj($("query", $("match_all", Json.obj()))));
		SearchResponse searchResponse = search.toCompletableFuture().get();
		assertThat(searchResponse.hits.total).isEqualTo(1);
		List<Person> people = searchResponse.hits.hitsAs(Person.read);
		ElasticTest.Person person = people.head();
		assertThat(person.name).isEqualTo("Jean Claude Dus");
	}

	@Test
	public void bulk_indexing_should_work() throws ExecutionException, InterruptedException {
		createIndexWithMapping();
		java.util.List<BulkResponse> response = Source.range(1, 500)
				.map(i -> BulkItem.create(INDEX, TYPE, String.valueOf(i), Json.obj().with("name", "name-" + i))).via(elasticClient.bulk(5, 2))
				.runWith(Sink.seq(), ActorMaterializer.create(system)).toCompletableFuture().get();

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
				.via(elasticClient.bulk(20, FiniteDuration.create(1, TimeUnit.SECONDS), 2)).runWith(Sink.seq(), ActorMaterializer.create(system))
				.toCompletableFuture().get();

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
		java.util.List<Either<Elastic.BulkFailure, BulkResponse>> response = Source.range(1, 500)
				.map(i -> BulkItem.create(INDEX, TYPE, String.valueOf(i), Json.obj().with("name", "name-" + i)))
				.via(elasticClient.bulkWithRetry(5, 2, 2, FiniteDuration.create(1, TimeUnit.SECONDS), Elastic.RetryMode.LineareLatency))
				.runWith(Sink.seq(), ActorMaterializer.create(system)).toCompletableFuture().get();

		assertThat(response).hasSize(100);
		elasticClient.refresh(INDEX).toCompletableFuture().get();

		Long count = elasticClient.count(INDEX).toCompletableFuture().get();
		assertThat(count).isEqualTo(500);
	}

	@Test
	public void bulk_indexing_with_retry_with_errors() throws ExecutionException, InterruptedException {
		createIndexWithMapping();
		Long start = System.currentTimeMillis();

		java.util.List<Either<Elastic.BulkFailure, BulkResponse>> results = Source.range(1, 10).concat(Source.range(1, 10))
				.map(i -> BulkItem.create(INDEX, TYPE, String.valueOf(i), Json.obj().with("name", "name-" + i)))
				.via(elasticClient.bulkWithRetry(5, 1, 2, FiniteDuration.create(1, TimeUnit.SECONDS), Elastic.RetryMode.ExponentialLatency,
						pair -> pair._1.isFailure() || (pair._1.isSuccess() && pair._1.get().errors)))
				.runWith(Sink.seq(), ActorMaterializer.create(system)).toCompletableFuture().get();

		Long stop = System.currentTimeMillis();

		Long length = 1000L * 6;

		assertThat(stop - start).isGreaterThan(length);

		assertThat(results).hasSize(4);

		List<Elastic.BulkFailure> onError = List.ofAll(results).filter(Either::isLeft).map(Either::getLeft);
		assertThat(onError).hasSize(2);
		onError.forEach(err -> assertThat(err.bulkResponse.getErrors()).hasSize(5));

		elasticClient.refresh(INDEX).toCompletableFuture().get();

		Long count = elasticClient.count(INDEX).toCompletableFuture().get();
		assertThat(count).isEqualTo(10);
	}

	@Test
    public void get_mapping() throws ExecutionException, InterruptedException {
        createIndexWithMapping();
        JsValue jsValue = elasticClient.getMapping(INDEX, TYPE).toCompletableFuture().get();
        assertThat(jsValue).isEqualTo(Json.obj(
                $("test",
                        $("mappings",
                                $("test",
                                        $("properties",
                                                $("name", Json.obj(
                                                        $("type", "keyword")
                                                ))
                                        )
                                )
                        )
                )
        ));
    }

    @Test
    public void get_settings() throws ExecutionException, InterruptedException {
        createIndexWithMapping();
        JsValue jsValue = elasticClient.getSettings(INDEX).toCompletableFuture().get();

        JsObject newObj = jsValue.asObject().mapProperties(t ->
                t._2.asObject().mapProperties(s ->
                        s._2.asObject().mapProperties(i ->
                                i._2.asObject()
                                        .remove("creation_date")
                                        .remove("version")
                                        .remove("uuid")
                        )
                )
        );

        assertThat(newObj).isEqualTo(Json.obj(
                $("test",
                        $("settings",
                                $("index", Json.obj(
                                        $("number_of_shards", "5"),
                                        $("provided_name", "test"),
                                        $("number_of_replicas", "1")
                                ))
                        )
                )
        ));
    }


    @Test
    public void update_settings() throws ExecutionException, InterruptedException {
        get_settings();

        elasticClient.updateSettings(INDEX, Json.obj(
			$("settings",
					$("index", Json.obj(
							$("number_of_replicas", "3")
					))
			)
        )).toCompletableFuture().get();
        JsValue jsValue = elasticClient.getSettings(INDEX).toCompletableFuture().get();

        JsObject newObj = jsValue.asObject().mapProperties(t ->
                t._2.asObject().mapProperties(s ->
                        s._2.asObject().mapProperties(i ->
                                i._2.asObject()
                                        .remove("creation_date")
                                        .remove("version")
                                        .remove("uuid")
                        )
                )
        );

        assertThat(newObj).isEqualTo(Json.obj(
                $("test",
                        $("settings",
                                $("index", Json.obj(
                                        $("number_of_shards", "5"),
                                        $("provided_name", "test"),
                                        $("number_of_replicas", "3")
                                ))
                        )
                )
        ));
    }


    @Test
	public void cat_indices() throws ExecutionException, InterruptedException {
		createIndexWithMapping();
		List<Map<String, String>> indices = elasticClient.cat("indices").toCompletableFuture().get();

		assertThat(indices).hasSize(1);
		Map<String, String> line = indices.head()
				.filter(t -> !t._1.equals("uuid") && !t._1.equals("health") && !t._1.equals("pri.store.size") && !t._1.equals("store.size"));
		assertThat(line).isEqualTo(HashMap.of("docs.deleted", "0", "pri", "5", "index", "test", "status", "open", "docs.count", "0", "rep", "1"));
	}

	@Test
	public void health() throws ExecutionException, InterruptedException {
		createIndexWithMapping();
		JsValue health = elasticClient.health().toCompletableFuture().get();
		assertThat(health.field("cluster_name").asString()).isEqualTo("elasticsearch");
	}


	@Test
	public void aliases() throws ExecutionException, InterruptedException  {
		createIndexWithMapping();

		assertThat(elasticClient.getAliases().toCompletableFuture().get()).isEqualTo(Json.obj($("test", $("aliases", Json.obj()))));
		assertThat(elasticClient.getAliases(INDEX).toCompletableFuture().get()).isEqualTo(Json.obj($("test", $("aliases", Json.obj()))));

		elasticClient.updateAliases(Json.obj($("actions", Json.arr(
				$( "add",  Json.obj($( "index", "test"), $("alias", "aliasTest" )))
		)))).toCompletableFuture().get();

		assertThat(elasticClient.getAliases().toCompletableFuture().get()).isEqualTo(Json.obj($("test", $("aliases", Json.obj($("aliasTest", Json.obj()))))));
		assertThat(elasticClient.getAliases(INDEX).toCompletableFuture().get()).isEqualTo(Json.obj($("test", $("aliases", Json.obj($("aliasTest", Json.obj()))))));

		elasticClient.deleteAlias(INDEX, "aliasTest").toCompletableFuture().get();

		assertThat(elasticClient.getAliases().toCompletableFuture().get()).isEqualTo(Json.obj($("test", $("aliases", Json.obj()))));
		assertThat(elasticClient.getAliases(INDEX).toCompletableFuture().get()).isEqualTo(Json.obj($("test", $("aliases", Json.obj()))));

		elasticClient.addAlias(INDEX, "aliasTest2").toCompletableFuture().get();

		assertThat(elasticClient.getAliases().toCompletableFuture().get()).isEqualTo(Json.obj($("test", $("aliases", Json.obj($("aliasTest2", Json.obj()))))));
		assertThat(elasticClient.getAliases(INDEX).toCompletableFuture().get()).isEqualTo(Json.obj($("test", $("aliases", Json.obj($("aliasTest2", Json.obj()))))));

	}

	@Test
	public void force_merge() throws ExecutionException, InterruptedException {
		createIndexWithMapping();
		elasticClient.forceMerge().toCompletableFuture().get();
		elasticClient.forceMerge(INDEX).toCompletableFuture().get();
		elasticClient.forceMerge(List.of(INDEX)).toCompletableFuture().get();
	}

	@Test
	public void reindex() throws ExecutionException, InterruptedException {
		createIndexWithMapping();
		String index = INDEX + "-2";
		createIndexWithMapping(index, "test");

		ElasticType elasticType = elasticClient.type(INDEX, TYPE);
		elasticType.index(Json.obj($("name", "Ragnar")), Option.some("1")).toCompletableFuture().get();
		elasticType.index(Json.obj($("name", "Floki")), Option.some("2")).toCompletableFuture().get();
		elasticType.refresh().toCompletableFuture().get();

		ElasticType newType = elasticClient.type(index, TYPE);
		assertThat(elasticType.count().toCompletableFuture().get()).isEqualTo(2);
		assertThat(newType.count().toCompletableFuture().get()).isEqualTo(0);

		elasticClient.reindex(Json.obj(
				$("source", $("index", INDEX)),
				$("dest", $("index", index))
		)).toCompletableFuture().get();
		newType.refresh().toCompletableFuture().get();

		assertThat(newType.count().toCompletableFuture().get()).isEqualTo(2);


	}

	// @Test
	// public void testJson() {
	// String json =
	// "{\"_index\":\"test\",\"_type\":\"test\",\"_id\":\"1\",\"_version\":1,\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0},\"created\":true}";
	//
	// JsValue parse = Json.parse(json);
	// JsResult<IndexResponse> indexResponses = Json.fromJson(parse, IndexResponse.reads);
	// IndexResponse indexResponse = indexResponses.get();
	//
	//
	// }

	@Test
	public void create_exists_get_delete_template() throws ExecutionException, InterruptedException {
		Boolean tplExists = elasticClient.templateExists("test").toCompletableFuture().get();
		assertThat(tplExists).isFalse();
		String template = "{" + "    \"template\" : \"te*\", " + "    \"settings\" : { " + "        \"number_of_shards\" : 1 " + "    }, "
				+ "    \"aliases\" : { " + "        \"alias1\" : {}, " + "        \"alias2\" : { " + "            \"filter\" : { "
				+ "                \"term\" : {\"user\" : \"kimchy\" } " + "            }, " + "            \"routing\" : \"kimchy\" " + "        }, "
				+ "        \"{index}-alias\" : {}  " + "    } " + "} ";
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
		createIndexWithMapping(INDEX, "test");
	}

	private void createIndexWithMapping(String index, String type) throws ExecutionException, InterruptedException {
		elasticClient
				.createIndex(index,
						Json.obj($("mappings",
								$(type,
										$("properties",
												$("name", Json.obj(
														$("type", "keyword")
												))
										)
								)
						))
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
		 * @param type
		 *            a type to match the argument against
		 * @param apply
		 *            an action to apply to the argument if the type matches
		 * @return a builder with the case statement added
		 */
		public static <P extends Throwable, T> PFBuilder<Throwable, T> match(final Class<P> type, FI.Apply<P, T> apply) {
			return Match.match(type, apply);
		}

		/**
		 * Return a new {@link PFBuilder} with a case statement added.
		 *
		 * @param type
		 *            a type to match the argument against
		 * @param predicate
		 *            a predicate that will be evaluated on the argument if the type matches
		 * @param apply
		 *            an action to apply to the argument if the type matches and the predicate returns true
		 * @return a builder with the case statement added
		 */
		public static <P extends Throwable, T> PFBuilder<Throwable, T> match(final Class<P> type, FI.TypedPredicate<P> predicate,
				FI.Apply<P, T> apply) {
			return Match.match(type, predicate, apply);
		}

		/**
		 * Return a new {@link PFBuilder} with a case statement added.
		 *
		 * @param apply
		 *            an action to apply to the argument
		 * @return a builder with the case statement added
		 */
		public static <T> PFBuilder<Throwable, T> matchAny(FI.Apply<Throwable, T> apply) {
			return Match.matchAny(apply);
		}
	}

}
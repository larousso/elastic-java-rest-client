package elastic;

import akka.NotUsed;
import akka.stream.javadsl.Flow;
import elastic.request.BulkItem;
import elastic.response.BulkResponse;
import elastic.response.GetResponse;
import elastic.response.IndexResponse;
import elastic.response.SearchResponse;
import javaslang.Function1;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.collection.HashMap;
import javaslang.collection.List;
import javaslang.collection.Seq;
import javaslang.control.Option;
import javaslang.control.Try;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;
import scala.concurrent.duration.FiniteDuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static javaslang.API.*;
import static javaslang.Patterns.None;
import static javaslang.Patterns.Some;

/**
 * Created by adelegue on 31/01/2017.
 */
public class Elastic implements Closeable {

    RestClient restClient;

    public Elastic(HttpHost... hosts) {
        this.restClient = RestClient.builder(hosts).build();
    }

    public Elastic(List<HttpHost> hosts) {
        this(hosts, Option.none(), Option.none());
    }

    public Elastic(List<HttpHost> hosts, Option<String> username, Option<String> password) {
        RestClientBuilder restClientBuilder = RestClient.builder(hosts.toJavaArray(HttpHost.class));

        Option<Tuple2<String, String>> usernameAndPassword = username.flatMap(u ->
                password.map(p -> Tuple.of(u, p))
        );

        this.restClient = usernameAndPassword
                .map(pair -> {
                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(pair._1, pair._2));
                    return restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
                })
                .getOrElse(restClientBuilder)
                .build();

    }

    public Elastic(RestClient restClient) {
        this.restClient = restClient;
    }

    @Override
    public void close() throws IOException {
        this.restClient.close();
    }

    public CompletionStage<GetResponse> get(String index, String type, String id) {
        String path = List.of(index, type, id).mkString("/");
        return request(path, "GET")
                .thenCompose(convert(GetResponse.read));
    }

    public CompletionStage<SearchResponse> search(JsValue query) {
        return search(Option.none(), Option.none(), query);
    }

    public CompletionStage<SearchResponse> search(String index, JsValue query) {
        return search(Option.of(index), Option.none(), query);
    }

    public CompletionStage<SearchResponse> search(String index, String type, JsValue query) {
        return search(Option.of(index), Option.of(type), query);
    }

    public CompletionStage<SearchResponse> search(Option<String> index, Option<String> type, JsValue query) {
        String path = "/" + List.of(index, type, Option.of("_search")).flatMap(identity()).mkString("/");
        return request(path, "POST", query)
                .thenCompose(convert(SearchResponse.read));
    }

    public CompletionStage<IndexResponse> index(String index, String type, JsValue data, Option<String> mayBeId) {
        return index(index, type, data, mayBeId, null, null, null);
    }

    public CompletionStage<IndexResponse> index(String index, String type, JsValue data, Option<String> mayBeId, Boolean create, String parent, Boolean refresh) {
        String basePath = "/" + index + "/" + type;

        List<Tuple2<String, String>> entries = List.of(
                Option.of(refresh).filter(Boolean.TRUE::equals).map(any -> Tuple.of("refresh", "true")),
                Option.of(parent).filter(Objects::nonNull).map(p -> Tuple.of("parent", p))
        ).flatMap(identity());

        HashMap<String, String> queryMap = HashMap.ofEntries(entries);

        return Match(mayBeId).of(
                Case(Some($()), id -> {
                    String p = basePath + "/" + id;
                    if(Boolean.TRUE.equals(create)) {
                        p = p + "_create";
                    }
                    return request(basePath + "/" + id, "PUT", data, queryMap);
                }),
                Case(None(), any -> request(basePath, "POST", data, queryMap))
        )
                .thenCompose(convert(IndexResponse.read));
    }


    public CompletionStage<JsValue> createIndex(String name, JsValue settings) {
        return request("/" + name, "PUT", settings)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> getIndex(String name) {
        return request("/" + name, "GET")
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> deleteIndex(String name) {
        return request("/" + name, "DELETE")
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> createMapping(String index, String type, JsValue mapping) {
        String path = "/" + List.of(index, "_mapping", type).mkString("/");
        return request(path, "PUT", mapping)
                .thenCompose(handleError());
    }

    public CompletionStage<Boolean> indexExists(String name) {
        String path = "/" + name;
        return rawRequest(path, "HEAD", Option.none(), HashMap.empty()).thenApply(exists());
    }

    public CompletionStage<Long> count() {
        return count(Option.none(), Option.none());
    }

    public CompletionStage<Long> count(String index) {
        return count(Option.of(index), Option.none());
    }

    public CompletionStage<Long> count(String index, String type) {
        return count(Option.of(index), Option.of(type));
    }

    public CompletionStage<Long> count(Option<String> index, Option<String> type) {
        return search(index, type, Json.obj()
                .with("size", 0)
                .with("query", Json.obj()
                        .with("match_all", Json.obj())
                )
        )
                .thenApply(searchResponse -> searchResponse.hits.total);
    }

    public CompletionStage<JsValue> refresh() {
        return refresh(List.empty());
    }

    public CompletionStage<JsValue> refresh(String index) {
        return refresh(List.of(index));
    }

    public CompletionStage<JsValue> refresh(List<String> indexes) {
        String path = "/";
        if(!indexes.isEmpty()) {
            path += indexes.mkString(",");
        }
        path += "/_refresh";
        return request(path, "POST").thenCompose(handleError());
    }


    public Flow<BulkItem, BulkResponse, NotUsed> bulk(Integer batchSize, Integer parallelisation) {
        return bulk(batchSize, null, parallelisation);
    }

    public Flow<BulkItem, BulkResponse, NotUsed> bulk(Integer batchSize, FiniteDuration within, Integer parallelisation) {
        String path = "/_bulk";

        Flow<BulkItem, java.util.List<BulkItem>, NotUsed> windows;
        if(within == null) {
            windows =  Flow.<BulkItem>create().filter(Objects::nonNull).grouped(batchSize);
        } else {
            windows = Flow.<BulkItem>create().filter(Objects::nonNull).groupedWithin(batchSize, within);
        }
        return windows
                .map(items -> List.ofAll(items)
                        .flatMap(i -> List.of(i.operation, i.source))
                        .filter(Objects::nonNull)
                        .map(Json::toJson)
                        .map(Json::stringify)
                        .mkString("\n") + "\n")
                .mapAsync(parallelisation, bulkBody -> request(path, "POST", bulkBody)
                                .thenCompose(convert(BulkResponse.reader))
                );
    }


    public CompletionStage<Boolean> templateExists(String name) {
        return rawRequest("/_template/" + name, "HEAD", Option.none(), HashMap.empty())
                .thenApply(exists());
    }

    private Function<Response, Boolean> exists() {
        return response -> {
            if(response.getStatusLine().getStatusCode() == 200) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        };
    }

    public CompletionStage<JsValue> getTemplate(String... name) {
        return request("/_template/" + List.of(name).mkString(","), "GET");
    }

    public CompletionStage<JsValue> createTemplate(String name, JsValue template) {
        return request("/_template/" + name, "PUT", template);
    }

    public CompletionStage<JsValue> deleteTemplate(String name) {
        return request("/_template/" + name, "DELETE");
    }

    private <T> Function<JsValue, CompletionStage<T>> fromJson(Reader<T> reader) {
        return jsValue ->
                Json.fromJson(jsValue, reader).fold(
                        error -> Elastic.<T>failed(new JsonInvalidException(error.errors)),
                        ok -> Elastic.success(ok.get())
                );
    }

    private Function<JsValue, CompletionStage<JsValue>> handleError() {
        return json -> {
            JsObject jsObject = json.asObject();
            if(jsObject.exists("error") && !jsObject.field("error").asObject().isEmpty()) {
                return Elastic.failed(new RuntimeException(json.stringify()));
            } else {
                return Elastic.success(json);
            }
        };
    }

    private <T> Function<JsValue, CompletionStage<T>> convert(Reader<T> reader) {
        return json -> handleError().apply(json).thenCompose(fromJson(reader));
    }

    private CompletionStage<JsValue> request(String path, String verb) {
        return request(path, verb, Option.none(), HashMap.empty());
    }

    private CompletionStage<JsValue> request(String path, String verb, String body) {
        return request(path, verb, Option.of(body), HashMap.empty());
    }

    private CompletionStage<JsValue> request(String path, String verb, JsValue body) {
        return request(path, verb, body, HashMap.empty());
    }

    private CompletionStage<JsValue> request(String path, String verb, JsValue body, HashMap<String, String> query) {
        return request(path, verb, Option.of(Json.stringify(body)), query);
    }

    private CompletionStage<JsValue> request(String path, String verb, Option<String> body, HashMap<String, String> query) {
        CompletionStage<Response> response = rawRequest(path, verb, body, query);

        return response
                .thenApply(Response::getEntity)
                .thenCompose(entity -> {
                    if(entity == null) {
                        return success(null);
                    } else {
                        return Try.of(() -> EntityUtils.toString(entity))
                                .map(Elastic::success)
                                .getOrElseGet(Elastic::failed);
                    }
                })
                .thenApply(json -> {
                    if(json == null) {
                        return new JsNull();
                    } else {
                        return Json.parse(json);
                    }
                });
    }

    private CompletionStage<Response> rawRequest(String path, String verb, Option<String> body, HashMap<String, String> query) {
        return body.map(b -> {
                HttpEntity entity = new NStringEntity(b, ContentType.APPLICATION_JSON);
                EsResponseListener esResponseListener = new EsResponseListener();
                restClient.performRequestAsync(verb, path, query.toJavaMap(), entity, esResponseListener);
                return esResponseListener.promise;
            }).getOrElse(() -> {
                EsResponseListener esResponseListener = new EsResponseListener();
                restClient.performRequestAsync(verb, path, query.toJavaMap(), esResponseListener);
                return esResponseListener.promise;
            });
    }

    private static <T> CompletionStage<T> success(T e) {
        return CompletableFuture.completedFuture(e);
    }

    private static <T> CompletionStage<T> failed(Throwable e) {
        CompletableFuture<T> cf = new CompletableFuture<T>();
        cf.completeExceptionally(e);
        return cf;
    }

    private <T> Function1<T, T> identity() {
        return e -> e;
    }


    private static class EsResponseListener implements ResponseListener {

        public final CompletableFuture<Response> promise;

        public EsResponseListener() {
            promise = new CompletableFuture<>();
        }

        @Override
        public void onSuccess(Response response) {
            promise.complete(response);
        }

        @Override
        public void onFailure(Exception exception) {
            promise.completeExceptionally(exception);
        }
    }

    private static String buildMessage(Seq<Throwable> errors) {
        return List.ofAll(errors).map(Throwable::getMessage).mkString(", ");
    }

    public final class JsonInvalidException extends RuntimeException {
        private final Seq<Throwable> errors;

        public JsonInvalidException(Seq<Throwable> errors) {
            super(buildMessage(errors));
            this.errors = errors;
        }

        public Seq<Throwable> errors() {
            return this.errors;
        }

    }

}

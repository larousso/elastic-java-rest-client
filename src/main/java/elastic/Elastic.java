package elastic;

import static javaslang.API.*;
import static javaslang.Patterns.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import akka.japi.Pair;
import elastic.streams.Flows;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.reactivecouchbase.json.JsObject;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;

import akka.NotUsed;
import akka.japi.function.Predicate;
import akka.stream.ThrottleMode;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
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
import javaslang.control.Either;
import javaslang.control.Option;
import javaslang.control.Try;
import scala.concurrent.duration.FiniteDuration;

import static org.reactivecouchbase.json.Syntax.*;

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

    public ElasticType type(String index, String type) {
        return new ElasticType(this, index, type);
    }

    public CompletionStage<GetResponse> get(String index, String type, String id) {
        String path = List.of(index, type, id).mkString("/");
        return request(path, "GET")
                .thenCompose(convert(GetResponse.reads));
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
                .thenCompose(convert(SearchResponse.reads));
    }

    public CompletionStage<IndexResponse> index(String index, String type, JsValue data, Option<String> mayBeId) {
        return index(index, type, data, mayBeId, null, null, null);
    }

    public CompletionStage<IndexResponse> index(String index, String type, JsValue data, Option<String> mayBeId, Boolean create, String parent, Boolean refresh) {
        //@formatter:off
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
                .thenCompose(convert(IndexResponse.reads));
        //@formatter:on
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

    public CompletionStage<JsValue> getMapping(String index, String type) {
        String path = "/" + List.of(index, "_mapping", type).mkString("/");
        return request(path, "GET")
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> getSettings(String index) {
        return request("/" + index + "/_settings", "GET")
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> updateSettings(String index, JsValue settings) {
        return request("/" + index + "/_settings", "PUT", settings)
                .thenCompose(handleError());
    }

    public CompletionStage<Boolean> indexExists(String name) {
        String path = "/" + name;
        return rawRequest(path, "HEAD", Option.none(), HashMap.empty()).thenApply(exists());
    }

    public CompletionStage<JsValue> updateAliases(JsValue aliases) {
        String path = "/_aliases";
        return request(path, "POST", aliases)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> addAlias(String index, String aliasName) {
        String path = "/" + index + "/_alias/" + aliasName;
        return request(path, "PUT")
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> deleteAlias(String index, String aliasName) {
        String path = "/" + index + "/_alias/" + aliasName;
        return request(path, "DELETE")
                .thenCompose(handleError());
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
        String path = indexes.mkString("/", ",", "/") + "_refresh";
        return request(path, "POST").thenCompose(handleError());
    }


    public CompletionStage<JsValue> forceMerge() {
        return forceMerge(List.empty());
    }

    public CompletionStage<JsValue> forceMerge(String index) {
        return forceMerge(List.of(index));
    }

    public CompletionStage<JsValue> forceMerge(List<String> indexes) {
        String path = indexes.mkString("/", ",", "/") + "_forcemerge";
        return request(path, "POST").thenCompose(handleError());
    }

    public CompletionStage<JsValue> reindex(JsObject reindex) {
        String path = "/_reindex";
        return request(path, "POST", reindex).thenCompose(handleError());
    }



    public Source<SearchResponse, NotUsed> scroll(String index, String type, JsValue searchQuery, Integer size, String scrollTime) {
        //@formatter:off
        return Source
                .fromCompletionStage(
                        request("/"+index+"/"+type+"/_search", "POST", searchQuery, HashMap.of("scroll", scrollTime)).thenCompose(convert(SearchResponse.reads))
                )
                .flatMapConcat(resp ->
                    Source.single(resp).concat(Source.unfoldAsync(resp._scroll_id, id -> this.nextScroll(id, scrollTime)))
                );
        //@formatter:on
    }

    private CompletionStage<Optional<Pair<String, SearchResponse>>> nextScroll(String scrollId, String scrollTime) {
        return request("/_search/scroll", "POST", Json.obj(
                    $("scroll", scrollTime),
                    $("scroll_id", scrollId)
                ))
                .thenCompose(convert(SearchResponse.reads))
                .thenApply(response -> {
                    if (response.hits.hits.isEmpty()) {
                        return Optional.<Pair<String, SearchResponse>>empty();
                    } else {
                        return Optional.of(Pair.create(response._scroll_id, response));
                    }
                });
    }



    public Flow<BulkItem, BulkResponse, NotUsed> bulk(Integer batchSize, Integer parallelisation) {
        return bulk(batchSize, null, parallelisation);
    }

    public Flow<BulkItem, BulkResponse, NotUsed> bulk(Integer batchSize, FiniteDuration within, Integer parallelisation) {
        Flow<BulkItem, java.util.List<BulkItem>, NotUsed> windows;
        if(within == null) {
            windows =  Flow.<BulkItem>create().filter(Objects::nonNull).grouped(batchSize);
        } else {
            windows = Flow.<BulkItem>create().filter(Objects::nonNull).groupedWithin(batchSize, within);
        }
        return windows.mapAsync(parallelisation, this::oneBulk).map(p -> p._1.get());
    }


    public Flow<BulkItem, BulkResponse, NotUsed> bulk(String index, String type, Integer batchSize, Integer parallelisation) {
        return bulk(index, type, batchSize, null, parallelisation);
    }

    public Flow<BulkItem, BulkResponse, NotUsed> bulk(String index, String type, Integer batchSize, FiniteDuration within, Integer parallelisation) {
        Flow<BulkItem, java.util.List<BulkItem>, NotUsed> windows;
        if(within == null) {
            windows =  Flow.<BulkItem>create().filter(Objects::nonNull).grouped(batchSize);
        } else {
            windows = Flow.<BulkItem>create().filter(Objects::nonNull).groupedWithin(batchSize, within);
        }
        return windows.mapAsync(parallelisation, i -> this.oneBulk(index, type, i)).map(p -> p._1.get());
    }

    public Flow<BulkItem, Either<BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(Integer batchSize, Integer parallelism, Integer nbRetry, FiniteDuration latency, RetryMode retryMode) {
        return bulkWithRetry(batchSize, null, parallelism, nbRetry, latency, retryMode);
    }

    public Flow<BulkItem, Either<BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(Integer batchSize, Integer parallelism, Integer nbRetry, FiniteDuration latency, RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        return bulkWithRetry(batchSize, null, parallelism, nbRetry, latency, retryMode, isError);
    }


    public Flow<BulkItem, Either<BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(Integer batchSize, FiniteDuration within, Integer parallelism, Integer nbRetry, FiniteDuration latency, RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        Flow<BulkItem, java.util.List<BulkItem>, NotUsed> windows;
        if(within == null) {
            windows =  Flow.<BulkItem>create().filter(Objects::nonNull).grouped(batchSize);
        } else {
            windows = Flow.<BulkItem>create().filter(Objects::nonNull).groupedWithin(batchSize, within);
        }
        return windows
                .flatMapMerge(parallelism, e ->
                        oneBulkWithRetry(e, nbRetry, latency, retryMode, isError)
                );
    }

    public Flow<BulkItem, Either<BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(Integer batchSize, FiniteDuration within, Integer parallelism, Integer nbRetry, FiniteDuration latency, RetryMode retryMode) {
        return bulkWithRetry(batchSize, within, parallelism, nbRetry, latency, retryMode, defaultIsBulkOnError());
    }


    public Flow<BulkItem, Either<BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(String index, String type, Integer batchSize, Integer parallelism, Integer nbRetry, FiniteDuration latency, RetryMode retryMode) {
        return bulkWithRetry(index, type, batchSize, null, parallelism, nbRetry, latency, retryMode);
    }

    public Flow<BulkItem, Either<BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(String index, String type, Integer batchSize, Integer parallelism, Integer nbRetry, FiniteDuration latency, RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        return bulkWithRetry(index, type, batchSize, null, parallelism, nbRetry, latency, retryMode, isError);
    }

    public Flow<BulkItem, Either<BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(String index, String type, Integer batchSize, FiniteDuration within, Integer parallelism, Integer nbRetry, FiniteDuration latency, RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        Flow<BulkItem, java.util.List<BulkItem>, NotUsed> windows;
        if(within == null) {
            windows =  Flow.<BulkItem>create().filter(Objects::nonNull).grouped(batchSize);
        } else {
            windows = Flow.<BulkItem>create().filter(Objects::nonNull).groupedWithin(batchSize, within);
        }
        return windows
                .flatMapMerge(parallelism, e ->
                        oneBulkWithRetry(index , type, e, nbRetry, latency, retryMode, isError)
                );
    }

    public Flow<BulkItem, Either<BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(String index, String type, Integer batchSize, FiniteDuration within, Integer parallelism, Integer nbRetry, FiniteDuration latency, RetryMode retryMode) {
        return bulkWithRetry(index, type, batchSize, within, parallelism, nbRetry, latency, retryMode, defaultIsBulkOnError());
    }

    private Predicate<Tuple2<Try<BulkResponse>, Response>> defaultIsBulkOnError() {
        return pair -> pair._2.getStatusLine().getStatusCode() == 503 || pair._2.getStatusLine().getStatusCode() == 429 || pair._1.isFailure();
    }

    public Source<Either<BulkFailure, BulkResponse>, NotUsed> oneBulkWithRetry(java.util.List<BulkItem> items, Integer nbRetry, FiniteDuration latency, RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        return oneBulkWithRetryInternal("/_bulk", items, nbRetry, latency, retryMode, isError);
    }

    public Source<Either<BulkFailure, BulkResponse>, NotUsed> oneBulkWithRetry(String index, String type, java.util.List<BulkItem> items, Integer nbRetry, FiniteDuration latency, RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        return oneBulkWithRetryInternal("/" + index + "/" + type + "/_bulk", items, nbRetry, latency, retryMode, isError);
    }

    private Source<Either<BulkFailure, BulkResponse>, NotUsed> oneBulkWithRetryInternal(String path, java.util.List<BulkItem> items, Integer nbRetry, FiniteDuration latency, RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        Flow<Integer, Integer, NotUsed> latencyFlow;
        if (retryMode == RetryMode.ExponentialLatency) {
            latencyFlow = Flow.<Integer>create().throttle(1, latency, 1, i -> i, ThrottleMode.shaping());
        } else if (retryMode == RetryMode.LineareLatency) {
            latencyFlow = Flow.<Integer>create().throttle(1, latency, 1, i -> 1, ThrottleMode.shaping());
        } else {
            latencyFlow = Flow.create();
        }
        //@formatter:off
        return Source.range(1, nbRetry)
                .via(latencyFlow)
                .via(Flows.mapAsync(any -> oneBulkInternal(items, path)))
                .filterNot(isError)
                .take(1)
                .orElse(Source.lazily(() -> Source.single(nbRetry + 1).via(latencyFlow).mapAsync(1, any -> oneBulk(items))))
                .map(r -> {
                    if(isError.test(r)) {
                        return Match(r._1).of(
                                Case(Success($()), s -> Either.left(BulkFailure.fromBulkResponse(s, r._2))),
                                Case(Failure($()), e -> Either.left(BulkFailure.fromException(e, r._2)))
                        );
                    } else {
                        return Either.right(r._1.get());
                    }
                });
        //@formatter:on
    }

    public CompletionStage<Tuple2<Try<BulkResponse>, Response>> oneBulk(java.util.List<BulkItem> items) {
        String path = "/_bulk";
        //@formatter:off
        return oneBulkInternal(items, path);
        //@formatter:on
    }


    public CompletionStage<Tuple2<Try<BulkResponse>, Response>> oneBulk(String index, String type, java.util.List<BulkItem> items) {
        String path = "/" + index + "/" + type + "/_bulk";
        //@formatter:off
        return oneBulkInternal(items, path);
        //@formatter:on
    }

    private CompletionStage<Tuple2<Try<BulkResponse>, Response>> oneBulkInternal(java.util.List<BulkItem> items, String path) {
        String bulkBody = List.ofAll(items)
                .flatMap(i -> List.of(i.operation, i.source))
                .filter(Objects::nonNull)
                .map(Json::toJson)
                .map(Json::stringify)
                .mkString("\n") + "\n";
        return requestWithResponse(path, "POST", bulkBody).thenCompose(p ->
                convert(BulkResponse.reads).apply(p._1).thenApply(r -> Tuple.of(Try.success(r), p._2))
                        .exceptionally(e -> {
                            if (e instanceof ResponseException) {
                                return Tuple.of(Try.failure(e), ((ResponseException) e).getResponse());
                            } else {
                                throw new RuntimeException(e);
                            }
                        })
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

    public CompletionStage<JsValue> request(String path, String verb) {
        return request(path, verb, Option.none(), HashMap.empty()).thenApply(Tuple2::_1);
    }

    public CompletionStage<JsValue> request(String path, String verb, String body) {
        return request(path, verb, Option.of(body), HashMap.empty()).thenApply(Tuple2::_1);
    }

    public CompletionStage<Tuple2<JsValue, Response>> requestWithResponse(String path, String verb, String body) {
        return request(path, verb, Option.of(body), HashMap.empty());
    }

    public CompletionStage<JsValue> request(String path, String verb, JsValue body) {
        return request(path, verb, body, HashMap.empty());
    }

    public CompletionStage<JsValue> request(String path, String verb, JsValue body, HashMap<String, String> query) {
        return request(path, verb, Option.of(Json.stringify(body)), query).thenApply(Tuple2::_1);
    }

    public CompletionStage<Tuple2<JsValue, Response>> request(String path, String verb, Option<String> body, HashMap<String, String> query) {
        CompletionStage<Response> response = rawRequest(path, verb, body, query);

        //@formatter:off
        return response
                .thenCompose(r -> {
                    HttpEntity entity = r.getEntity();
                    if(entity == null) {
                        return success(null);
                    } else {
                        return Try.of(() -> EntityUtils.toString(entity))
                                .map(s -> Tuple.of(s, r))
                                .map(Elastic::success)
                                .getOrElseGet(Elastic::failed);
                    }
                })
                .thenApply(pair -> pair.map1(json -> {
                            if(json == null) {
                                return null;
                            } else {
                                return Json.parse(json);
                            }
                        })
                );
        //@formatter:on
    }

    public CompletionStage<Response> rawRequest(String path, String verb, Option<String> body, HashMap<String, String> query) {
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

    public enum RetryMode {
        ExponentialLatency, LineareLatency, NoLatency
    }

    public static class BulkFailure extends RuntimeException {

        public final BulkResponse bulkResponse;
        public final Response rawResponse;
        public final Throwable cause;

        private BulkFailure(BulkResponse bulkResponse, Response rawResponse, Throwable cause) {
            super(cause);
            this.bulkResponse = bulkResponse;
            this.rawResponse = rawResponse;
            this.cause = cause;
        }

        public static BulkFailure fromException(Throwable cause, Response rawResponse) {
            return new BulkFailure(null, rawResponse, cause);
        }

        public static BulkFailure fromBulkResponse(BulkResponse bulkResponse, Response rawResponse) {
            return new BulkFailure(bulkResponse, rawResponse, null);
        }

        public BulkResponse bulkResponse() {
            return bulkResponse;
        }

        public Response rawResponse() {
            return rawResponse;
        }

        public Throwable cause() {
            return cause;
        }
    }
}

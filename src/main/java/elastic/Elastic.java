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
import javaslang.collection.*;
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
import javaslang.collection.Map;
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
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(id, "Id is null");
        String path = List.of(index, type, id).mkString("/");
        return get(path)
                .thenCompose(convert(GetResponse.reads));
    }

    public CompletionStage<SearchResponse> search(JsValue query) {
        Objects.requireNonNull(query, "Query is null");
        return search(Option.none(), Option.none(), query);
    }

    public CompletionStage<SearchResponse> search(String index, JsValue query) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(query, "Query is null");
        return search(Option.of(index), Option.none(), query);
    }

    public CompletionStage<SearchResponse> search(String index, String type, JsValue query) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(query, "Query is null");
        return search(Option.of(index), Option.of(type), query);
    }

    public CompletionStage<SearchResponse> search(Option<String> index, Option<String> type, JsValue query) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(query, "Query is null");
        String path = "/" + List.of(index, type, Option.of("_search")).flatMap(identity()).mkString("/");
        return post(path, query)
                .thenCompose(convert(SearchResponse.reads));
    }

    public CompletionStage<IndexResponse> index(String index, String type, JsValue data, Option<String> mayBeId) {
        return index(index, type, data, mayBeId, Boolean.FALSE, null, null);
    }

    public CompletionStage<IndexResponse> index(String index, String type, JsValue data, Option<String> mayBeId, Boolean create, String parent, Boolean refresh) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(data, "Data is null");
        Objects.requireNonNull(mayBeId, "Id is null");
        //@formatter:off
        String basePath = "/" + index + "/" + type;

        List<Tuple2<String, String>> entries = List.of(
                Option.of(refresh).filter(Boolean.TRUE::equals).map(any -> Tuple.of("refresh", "true")),
                Option.of(parent).filter(Objects::nonNull).map(p -> Tuple.of("parent", p))
        ).flatMap(identity());

        Map<String, String> queryMap = HashMap.ofEntries(entries);

        return Match(mayBeId).of(
                Case(Some($()), id -> {
                    String p = basePath + "/" + id;
                    if(Boolean.TRUE.equals(create)) {
                        p = p + "_create";
                    }
                    return put(p, data, queryMap);
                }),
                Case(None(), any -> post(basePath, data, queryMap))
        )
                .thenCompose(convert(IndexResponse.reads));
        //@formatter:on
    }


    public CompletionStage<JsValue> delete(String index, String type, String id) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(id, "id is null");
        return delete("/" + index  + "/" + type + "/" + id);
    }


    public CompletionStage<JsValue> createIndex(String index, JsValue settings) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(settings, "Settings is null");
        return put("/" + index, settings)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> getIndex(String index) {
        Objects.requireNonNull(index, "Index is null");
        return get("/" + index)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> deleteIndex(String index) {
        Objects.requireNonNull(index, "Index is null");
        return delete("/" + index)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> createMapping(String index, String type, JsValue mapping) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(mapping, "Mapping is null");
        String path = "/" + List.of(index, "_mapping", type).mkString("/");
        return put(path, mapping)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> getMapping(String index, String type) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        String path = "/" + List.of(index, "_mapping", type).mkString("/");
        return get(path)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> getSettings(String index) {
        Objects.requireNonNull(index, "Index is null");
        return get("/" + index + "/_settings")
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> updateSettings(String index, JsValue settings) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(settings, "Settings is null");
        return put("/" + index + "/_settings", settings)
                .thenCompose(handleError());
    }

    public CompletionStage<Boolean> indexExists(String index) {
        Objects.requireNonNull(index, "Index is null");
        String path = "/" + index;
        return headStatus(path).thenApply(exists());
    }

    public CompletionStage<Boolean> mappingExists(String index, String type) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        String path = "/" + index + "/_mapping/" + type;
        return headStatus(path).thenApply(exists());
    }


    public CompletionStage<JsValue> getAliases() {
        String path = "/_alias";
        return get(path)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> getAliases(String index) {
        Objects.requireNonNull(index, "Index is null");
        String path = "/" + index + "/_alias";
        return get(path)
                .thenCompose(handleError());
    }


    public CompletionStage<JsValue> updateAliases(JsValue aliases) {
        Objects.requireNonNull(aliases, "Aliases is null");
        String path = "/_aliases";
        return post(path, aliases)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> addAlias(String index, String aliasName) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(aliasName, "Alias name is null");
        String path = "/" + index + "/_alias/" + aliasName;
        return put(path)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> deleteAlias(String index, String aliasName) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(aliasName, "Alias name is null");
        String path = "/" + index + "/_alias/" + aliasName;
        return delete(path)
                .thenCompose(handleError());
    }

    public CompletionStage<JsValue> health() {
        return health(List.empty(), HashMap.empty());
    }

    public CompletionStage<JsValue> health(Map<String, String> querys) {
        return health(List.empty(), querys);
    }

    public CompletionStage<JsValue> health(List<String> indices, Map<String, String> querys) {
        Objects.requireNonNull(indices, "Indices name is null");
        Objects.requireNonNull(querys, "Querys name is null");
        String path = "/_cluster/health" + indices.mkString("/", ",", "");
        return get(path, querys)
                .thenCompose(handleError());
    }

    public CompletionStage<List<Map<String, String>>> cat(String operation) {
        Objects.requireNonNull(operation, "Operation name is null");
        return rawRequest("/_cat/" + operation, "GET", Option.none(), HashMap.of("v", ""))
                .thenCompose(this::readEntityAsString)
                .thenApply(Tuple2::_1)
                .thenApply(this::convertCat);
    }

    private List<Map<String, String>> convertCat(String str) {
        Option<List<Map<String, String>>> mayDatas = List.of(str.split("\n")).pop2Option().map(pair -> {
            List<String> head = List.of(pair._1.split("\\s+"));
            return pair._2.map(line -> {
                List<String> column = List.of(line.split("\\s+"));
                List<Tuple2<String, String>> zip = head.zip(column);
                return HashMap.ofEntries(zip);
            });
        });
        return mayDatas.getOrElse(List::<Map<String, String>>empty);
    }

    public CompletionStage<Long> count() {
        return count(Option.none(), Option.none());
    }

    public CompletionStage<Long> count(String index) {
        Objects.requireNonNull(index, "Index is null");
        return count(Option.of(index), Option.none());
    }

    public CompletionStage<Long> count(String index, String type) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        return count(Option.of(index), Option.of(type));
    }

    public CompletionStage<Long> count(Option<String> index, Option<String> type) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
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
        Objects.requireNonNull(index, "Index is null");
        return refresh(List.of(index));
    }

    public CompletionStage<JsValue> refresh(List<String> indices) {
        Objects.requireNonNull(indices, "Indices is null");
        String path = indices.mkString("/", ",", "/") + "_refresh";
        return post(path).thenCompose(handleError());
    }


    public CompletionStage<JsValue> forceMerge() {
        return forceMerge(List.empty());
    }

    public CompletionStage<JsValue> forceMerge(String index) {
        Objects.requireNonNull(index, "Index is null");
        return forceMerge(List.of(index));
    }

    public CompletionStage<JsValue> forceMerge(List<String> indices) {
        Objects.requireNonNull(indices, "Indices is null");
        String path = indices.mkString("/", ",", "/") + "_forcemerge";
        return post(path).thenCompose(handleError());
    }

    public CompletionStage<JsValue> reindex(JsObject reindex) {
        Objects.requireNonNull(reindex, "Reindex is null");
        String path = "/_reindex";
        return post(path, reindex).thenCompose(handleError());
    }


    public Source<SearchResponse, NotUsed> scroll(String index, String type, JsValue searchQuery, String scrollTime) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(searchQuery, "searchQuery is null");
        Objects.requireNonNull(scrollTime, "scrollTime is null");
        //@formatter:off
        return Source
                .fromCompletionStage(
                        post("/"+index+"/"+type+"/_search", searchQuery, HashMap.of("scroll", scrollTime)).thenCompose(convert(SearchResponse.reads))
                )
                .flatMapConcat(resp ->
                        Source.single(resp).concat(Source.unfoldAsync(resp._scroll_id, id -> this.nextScroll(id, scrollTime)))
                );
        //@formatter:on
    }

    private CompletionStage<Optional<Pair<String, SearchResponse>>> nextScroll(String scrollId, String scrollTime) {
        return post("/_search/scroll", Json.obj(
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
        Objects.requireNonNull(batchSize, "batchSize is null");
        Objects.requireNonNull(parallelisation, "parallelisation is null");
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
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(batchSize, "batchSize is null");
        Objects.requireNonNull(parallelisation, "parallelisation is null");
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
        Objects.requireNonNull(batchSize, "batchSize is null");
        Objects.requireNonNull(parallelism, "parallelism is null");
        Objects.requireNonNull(nbRetry, "nbRetry is null");
        Objects.requireNonNull(latency, "latency is null");
        Objects.requireNonNull(retryMode, "retryMode is null");
        Objects.requireNonNull(isError, "isError is null");

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
        Objects.requireNonNull(index, "index is null");
        Objects.requireNonNull(type, "type is null");
        Objects.requireNonNull(batchSize, "batchSize is null");
        Objects.requireNonNull(parallelism, "parallelism is null");
        Objects.requireNonNull(nbRetry, "nbRetry is null");
        Objects.requireNonNull(latency, "latency is null");
        Objects.requireNonNull(retryMode, "retryMode is null");
        Objects.requireNonNull(isError, "isError is null");

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
        Objects.requireNonNull(nbRetry, "nbRetry is null");
        Objects.requireNonNull(latency, "latency is null");
        Objects.requireNonNull(retryMode, "retryMode is null");
        Objects.requireNonNull(isError, "isError is null");

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


    public CompletionStage<Boolean> templateExists(String index) {
        Objects.requireNonNull(index, "index is null");
        return headStatus("/_template/" + index)
                .thenApply(exists());
    }

    private Function<Integer, Boolean> exists() {
        return status -> {
            if(status != 404) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        };
    }

    public CompletionStage<JsValue> getTemplate(String... name) {
        Objects.requireNonNull(name, "name is null");
        return get("/_template/" + List.of(name).mkString(","));
    }

    public CompletionStage<JsValue> createTemplate(String name, JsValue template) {
        Objects.requireNonNull(name, "name is null");
        Objects.requireNonNull(template, "template is null");
        return put("/_template/" + name, template);
    }

    public CompletionStage<JsValue> deleteTemplate(String name) {
        Objects.requireNonNull(name, "name is null");
        return delete("/_template/" + name);
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

    public CompletionStage<Tuple2<JsValue, Response>> requestWithResponse(String path, String verb, String body) {
        return request(path, verb, Option.of(body), HashMap.empty());
    }

    public CompletionStage<Integer> headStatus(String path) {
        return headStatus(path, HashMap.empty());
    }

    public CompletionStage<Integer> headStatus(String path, Map<String, String> query) {
        return rawRequest(path, "HEAD", Option.none(), query).thenApply(response ->
                response.getStatusLine().getStatusCode()
        );
    }

    public CompletionStage<JsValue> get(String path) {
        return get(path, HashMap.empty());
    }
    public CompletionStage<JsValue> get(String path, Map<String, String> query) {
        return request(path, "GET", Option.none(), query).thenApply(Tuple2::_1);
    }

    public CompletionStage<JsValue> delete(String path) {
        return delete(path, HashMap.empty());
    }

    public CompletionStage<JsValue> delete(String path, Map<String, String> query) {
        return request(path, "DELETE", Option.none(), query).thenApply(Tuple2::_1);
    }

    public CompletionStage<JsValue> post(String path, JsValue body, Map<String, String> query) {
        return post(path, Option.of(Json.stringify(body)), query);
    }

    public CompletionStage<JsValue> post(String path, JsValue body) {
        return post(path, Option.of(Json.stringify(body)), HashMap.empty());
    }

    public CompletionStage<JsValue> post(String path) {
        return post(path, Option.none(), HashMap.empty());
    }

    public CompletionStage<JsValue> post(String path, Map<String, String> query) {
        return post(path, Option.none(), query);
    }

    public CompletionStage<JsValue> post(String path, String body, Map<String, String> query) {
        return post(path, Option.of(body), query);
    }

    public CompletionStage<JsValue> post(String path, Option<String> body, Map<String, String> query) {
        return request(path, "POST", body, query).thenApply(Tuple2::_1);
    }

    public CompletionStage<JsValue> put(String path, JsValue body) {
        return put(path, Option.of(Json.stringify(body)), HashMap.empty());
    }

    public CompletionStage<JsValue> put(String path, JsValue body, Map<String, String> query) {
        return put(path, Option.of(Json.stringify(body)), query);
    }

    public CompletionStage<JsValue> put(String path) {
        return put(path, Option.none(), HashMap.empty());
    }

    public CompletionStage<JsValue> put(String path, Map<String, String> query) {
        return put(path, Option.none(), query);
    }

    public CompletionStage<JsValue> put(String path, String body, Map<String, String> query) {
        return put(path, Option.of(body), query);
    }

    public CompletionStage<JsValue> put(String path, Option<String> body, Map<String, String> query) {
        return request(path, "PUT", body, query).thenApply(Tuple2::_1);
    }

    public CompletionStage<Tuple2<JsValue, Response>> request(String path, String verb, Option<String> body, Map<String, String> query) {
        CompletionStage<Response> response = rawRequest(path, verb, body, query);

        //@formatter:off
        return response
                .thenCompose(this::readEntityAsString)
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

    private CompletionStage<Tuple2<String, Response>> readEntityAsString(Response r) {
        HttpEntity entity = r.getEntity();
        if(entity == null) {
            return success(null);
        } else {
            return Try.of(() -> EntityUtils.toString(entity))
                    .map(s -> Tuple.of(s, r))
                    .map(Elastic::success)
                    .getOrElseGet(Elastic::failed);
        }
    }

    public CompletionStage<Response> rawRequest(String path, String verb, Option<String> body, Map<String, String> query) {
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

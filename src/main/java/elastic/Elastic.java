package elastic;

import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.Patterns.*;
import static org.reactivecouchbase.json.Syntax.$;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

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
import akka.japi.Pair;
import akka.japi.function.Predicate;
import akka.stream.ThrottleMode;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import elastic.request.BulkItem;
import elastic.response.BulkResponse;
import elastic.response.GetResponse;
import elastic.response.IndexResponse;
import elastic.response.SearchResponse;
import elastic.streams.Flows;
import io.vavr.Function1;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.concurrent.Future;
import io.vavr.concurrent.Promise;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import scala.compat.java8.DurationConverters;
import scala.concurrent.duration.FiniteDuration;

public class Elastic implements Closeable {

    RestClient restClient;

    public Elastic(HttpHost... hosts) {
        this.restClient = RestClient.builder(hosts).build();
    }

    public Elastic(List<HttpHost> hosts) {
        this(hosts, Option.none(), Option.none());
    }

    public Elastic(List<HttpHost> hosts, Option<String> username, Option<String> password) {
        this(Settings.create(hosts).withUsername(username).withPassword(password));
    }

    public Elastic(Settings settings) {
        RestClientBuilder restClientBuilder = RestClient.builder(settings.hosts.toJavaArray(HttpHost[]::new))
                .setRequestConfigCallback(requestConfigBuilder ->
                        requestConfigBuilder.setConnectTimeout(settings.connectionTimeout).setSocketTimeout(settings.socketTimeout)
                ).setMaxRetryTimeoutMillis(settings.maxRetryTimeout);

        Option<Tuple2<String, String>> usernameAndPassword = settings.username.flatMap(u ->
                settings.password.map(p -> Tuple.of(u, p))
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

    public Future<GetResponse> get(String index, String type, String id) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(id, "Id is null");
        String path = List.of(index, type, id).mkString("/");
        return get(path)
                .flatMap(convert(GetResponse.reads));
    }

    public Future<SearchResponse> search(JsValue query) {
        Objects.requireNonNull(query, "Query is null");
        return search(Option.none(), Option.none(), query);
    }

    public Future<SearchResponse> search(String index, JsValue query) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(query, "Query is null");
        return search(Option.of(index), Option.none(), query);
    }

    public Future<SearchResponse> search(String index, String type, JsValue query) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(query, "Query is null");
        return search(Option.of(index), Option.of(type), query);
    }

    public Future<SearchResponse> search(Option<String> index, Option<String> type, JsValue query) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(query, "Query is null");
        String path = "/" + List.of(index, type, Option.of("_search")).flatMap(identity()).mkString("/");
        return post(path, query)
                .flatMap(convert(SearchResponse.reads));
    }

    public Future<IndexResponse> index(String index, String type, JsValue data, Option<String> mayBeId) {
        return index(index, type, data, mayBeId, Boolean.FALSE, null, null);
    }

    public Future<IndexResponse> index(String index, String type, JsValue data, Option<String> mayBeId, Boolean create, String parent, Boolean refresh) {
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
                Case($Some($()), id -> {
                    String p = basePath + "/" + id;
                    if(Boolean.TRUE.equals(create)) {
                        p = p + "/_create";
                    }
                    return put(p, data, queryMap);
                }),
                Case($None(), any -> post(basePath, data, queryMap))
        )
                .flatMap(convert(IndexResponse.reads));
        //@formatter:on
    }


    public Future<JsValue> delete(String index, String type, String id) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(id, "id is null");
        return delete("/" + index  + "/" + type + "/" + id);
    }


    public Future<JsValue> createIndex(String index, JsValue settings) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(settings, "Settings is null");
        return put("/" + index, settings)
                .flatMap(handleError());
    }

    public Future<JsValue> getIndex(String index) {
        Objects.requireNonNull(index, "Index is null");
        return get("/" + index)
                .flatMap(handleError());
    }

    public Future<JsValue> deleteIndex(String index) {
        Objects.requireNonNull(index, "Index is null");
        return delete("/" + index)
                .flatMap(handleError());
    }

    public Future<JsValue> createMapping(String index, String type, JsValue mapping) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(mapping, "Mapping is null");
        String path = "/" + List.of(index, "_mapping", type).mkString("/");
        return put(path, mapping)
                .flatMap(handleError());
    }

    public Future<JsValue> getMapping(String index, String type) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        String path = "/" + List.of(index, "_mapping", type).mkString("/");
        return get(path)
                .flatMap(handleError());
    }

    public Future<JsValue> getSettings(String index) {
        Objects.requireNonNull(index, "Index is null");
        return get("/" + index + "/_settings")
                .flatMap(handleError());
    }

    public Future<JsValue> updateSettings(String index, JsValue settings) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(settings, "Settings is null");
        return put("/" + index + "/_settings", settings)
                .flatMap(handleError());
    }

    public Future<Boolean> indexExists(String index) {
        Objects.requireNonNull(index, "Index is null");
        String path = "/" + index;
        return headStatus(path).map(exists());
    }

    public Future<Boolean> mappingExists(String index, String type) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        String path = "/" + index + "/_mapping/" + type;
        return headStatus(path).map(exists());
    }


    public Future<JsValue> getAliases() {
        String path = "/_alias";
        return get(path)
                .flatMap(handleError());
    }

    public Future<JsValue> getAliases(String index) {
        Objects.requireNonNull(index, "Index is null");
        String path = "/" + index + "/_alias";
        return get(path)
                .flatMap(handleError());
    }


    public Future<JsValue> updateAliases(JsValue aliases) {
        Objects.requireNonNull(aliases, "Aliases is null");
        String path = "/_aliases";
        return post(path, aliases)
                .flatMap(handleError());
    }

    public Future<JsValue> addAlias(String index, String aliasName) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(aliasName, "Alias name is null");
        String path = "/" + index + "/_alias/" + aliasName;
        return put(path)
                .flatMap(handleError());
    }

    public Future<JsValue> deleteAlias(String index, String aliasName) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(aliasName, "Alias name is null");
        String path = "/" + index + "/_alias/" + aliasName;
        return delete(path)
                .flatMap(handleError());
    }

    public Future<JsValue> health() {
        return health(List.empty(), HashMap.empty());
    }

    public Future<JsValue> health(Map<String, String> querys) {
        return health(List.empty(), querys);
    }

    public Future<JsValue> health(List<String> indices, Map<String, String> querys) {
        Objects.requireNonNull(indices, "Indices name is null");
        Objects.requireNonNull(querys, "Querys name is null");
        String path = "/_cluster/health" + indices.mkString("/", ",", "");
        return get(path, querys)
                .flatMap(handleError());
    }

    public Future<List<Map<String, String>>> cat(String operation) {
        Objects.requireNonNull(operation, "Operation name is null");
        return rawRequest("/_cat/" + operation, "GET", Option.none(), HashMap.of("v", ""))
                .flatMap(this::readEntityAsString)
                .map(Tuple2::_1)
                .map(this::convertCat);
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

    public Future<Long> count() {
        return count(Option.none(), Option.none());
    }

    public Future<Long> count(String index) {
        Objects.requireNonNull(index, "Index is null");
        return count(Option.of(index), Option.none());
    }

    public Future<Long> count(String index, String type) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        return count(Option.of(index), Option.of(type));
    }

    public Future<Long> count(Option<String> index, Option<String> type) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        return search(index, type, Json.obj()
                .with("size", 0)
                .with("query", Json.obj()
                        .with("match_all", Json.obj())
                )
        )
                .map(searchResponse -> searchResponse.hits.total);
    }

    public Future<JsValue> refresh() {
        return refresh(List.empty());
    }

    public Future<JsValue> refresh(String index) {
        Objects.requireNonNull(index, "Index is null");
        return refresh(List.of(index));
    }

    public Future<JsValue> refresh(List<String> indices) {
        Objects.requireNonNull(indices, "Indices is null");
        String path = indices.mkString("/", ",", "/") + "_refresh";
        return post(path).flatMap(handleError());
    }


    public Future<JsValue> forceMerge() {
        return forceMerge(List.empty());
    }

    public Future<JsValue> forceMerge(String index) {
        Objects.requireNonNull(index, "Index is null");
        return forceMerge(List.of(index));
    }

    public Future<JsValue> forceMerge(List<String> indices) {
        Objects.requireNonNull(indices, "Indices is null");
        String path = indices.mkString("/", ",", "/") + "_forcemerge";
        return post(path).flatMap(handleError());
    }

    public Future<JsValue> reindex(JsObject reindex) {
        Objects.requireNonNull(reindex, "Reindex is null");
        String path = "/_reindex";
        return post(path, reindex).flatMap(handleError());
    }


    public Source<SearchResponse, NotUsed> scroll(String index, String type, JsValue searchQuery, String scrollTime) {
        Objects.requireNonNull(index, "Index is null");
        Objects.requireNonNull(type, "Type is null");
        Objects.requireNonNull(searchQuery, "searchQuery is null");
        Objects.requireNonNull(scrollTime, "scrollTime is null");
        //@formatter:off
        return Source
                .fromCompletionStage(
                        post("/"+index+"/"+type+"/_search", searchQuery, HashMap.of("scroll", scrollTime)).flatMap(convert(SearchResponse.reads)).toCompletableFuture()
                )
                .flatMapConcat(resp ->
                        Source.single(resp).concat(Source.unfoldAsync(resp._scroll_id, id -> this.nextScroll(id, scrollTime).toCompletableFuture()))
                );
        //@formatter:on
    }

    private Future<Optional<Pair<String, SearchResponse>>> nextScroll(String scrollId, String scrollTime) {
        return post("/_search/scroll", Json.obj(
                $("scroll", scrollTime),
                $("scroll_id", scrollId)
        ))
                .flatMap(convert(SearchResponse.reads))
                .map(response -> {
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
            windows = Flow.<BulkItem>create().filter(Objects::nonNull).groupedWithin(batchSize, DurationConverters.toJava(within));
        }
        return windows.mapAsync(parallelisation, elt -> this.oneBulk(elt).toCompletableFuture()).map(p -> p._1.get());
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
            windows = Flow.<BulkItem>create().filter(Objects::nonNull).groupedWithin(batchSize, DurationConverters.toJava(within));
        }
        return windows.mapAsync(parallelisation, i -> this.oneBulk(index, type, i).toCompletableFuture()).map(p -> p._1.get());
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
            windows = Flow.<BulkItem>create().filter(Objects::nonNull).groupedWithin(batchSize, DurationConverters.toJava(within));
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
            windows = Flow.<BulkItem>create().filter(Objects::nonNull).groupedWithin(batchSize, DurationConverters.toJava(within));
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
            latencyFlow = Flow.<Integer>create().throttle(1, DurationConverters.toJava(latency), 1, i -> i, ThrottleMode.shaping());
        } else if (retryMode == RetryMode.LineareLatency) {
            latencyFlow = Flow.<Integer>create().throttle(1, DurationConverters.toJava(latency), 1, i -> 1, ThrottleMode.shaping());
        } else {
            latencyFlow = Flow.create();
        }
        //@formatter:off
        return Source.range(1, nbRetry)
                .via(latencyFlow)
                .via(Flows.mapAsync(any -> oneBulkInternal(items, path).toCompletableFuture()))
                .filterNot(isError)
                .take(1)
                .orElse(Source.lazySource(() -> Source.single(nbRetry + 1).via(latencyFlow).mapAsync(1, any -> oneBulk(items).toCompletableFuture())))
                .map(r -> {
                    if(isError.test(r)) {
                        return Match(r._1).of(
                                Case($Success($()), s -> Either.left(BulkFailure.fromBulkResponse(s, r._2))),
                                Case($Failure($()), e -> Either.left(BulkFailure.fromException(e, r._2)))
                        );
                    } else {
                        return Either.right(r._1.get());
                    }
                });
        //@formatter:on
    }

    public Future<Tuple2<Try<BulkResponse>, Response>> oneBulk(java.util.List<BulkItem> items) {
        String path = "/_bulk";
        //@formatter:off
        return oneBulkInternal(items, path);
        //@formatter:on
    }


    public Future<Tuple2<Try<BulkResponse>, Response>> oneBulk(String index, String type, java.util.List<BulkItem> items) {
        String path = "/" + index + "/" + type + "/_bulk";
        //@formatter:off
        return oneBulkInternal(items, path);
        //@formatter:on
    }

    private Future<Tuple2<Try<BulkResponse>, Response>> oneBulkInternal(java.util.List<BulkItem> items, String path) {
        String bulkBody = List.ofAll(items)
                .flatMap(i -> List.of(i.operation, i.source))
                .filter(Objects::nonNull)
                .map(Json::toJson)
                .map(Json::stringify)
                .mkString("\n") + "\n";
        return requestWithResponse(path, "POST", bulkBody)
                .flatMap(p ->
                    convert(BulkResponse.reads).apply(p._1).map(r -> Tuple.of(Try.success(r), p._2))
                        .recover(e -> {
                            if (e instanceof ResponseException) {
                                return Tuple.of(Try.failure(e), ((ResponseException) e).getResponse());
                            } else {
                                throw new RuntimeException(e);
                            }
                        })
        );
    }


    public Future<Boolean> templateExists(String index) {
        Objects.requireNonNull(index, "index is null");
        return headStatus("/_template/" + index)
                .map(exists());
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

    public Future<JsValue> getTemplate(String... name) {
        Objects.requireNonNull(name, "name is null");
        return get("/_template/" + List.of(name).mkString(","));
    }

    public Future<JsValue> createTemplate(String name, JsValue template) {
        Objects.requireNonNull(name, "name is null");
        Objects.requireNonNull(template, "template is null");
        return put("/_template/" + name, template);
    }

    public Future<JsValue> deleteTemplate(String name) {
        Objects.requireNonNull(name, "name is null");
        return delete("/_template/" + name);
    }

    private <T> Function<JsValue, Future<T>> fromJson(Reader<T> reader) {
        return jsValue ->
                Json.fromJson(jsValue, reader).fold(
                        error -> Future.<T>failed(new JsonInvalidException(error.errors)),
                        ok -> Future.successful(ok.get())
                );
    }

    private Function<JsValue, Future<JsValue>> handleError() {
        return json -> {
            JsObject jsObject = json.asObject();
            if(jsObject.exists("error") && !jsObject.field("error").asObject().isEmpty()) {
                return Future.failed(new RuntimeException(json.stringify()));
            } else {
                return Future.successful(json);
            }
        };
    }

    private <T> Function<JsValue, Future<T>> convert(Reader<T> reader) {
        return json -> handleError().apply(json).flatMap(fromJson(reader));
    }

    public Future<Tuple2<JsValue, Response>> requestWithResponse(String path, String verb, String body) {
        return request(path, verb, Option.of(body), HashMap.empty());
    }

    public Future<Integer> headStatus(String path) {
        return headStatus(path, HashMap.empty());
    }

    public Future<Integer> headStatus(String path, Map<String, String> query) {
        return rawRequest(path, "HEAD", Option.none(), query).map(response ->
                response.getStatusLine().getStatusCode()
        );
    }

    public Future<JsValue> get(String path) {
        return get(path, HashMap.empty());
    }
    public Future<JsValue> get(String path, Map<String, String> query) {
        return request(path, "GET", Option.none(), query).map(Tuple2::_1);
    }

    public Future<JsValue> delete(String path) {
        return delete(path, HashMap.empty());
    }

    public Future<JsValue> delete(String path, Map<String, String> query) {
        return request(path, "DELETE", Option.none(), query).map(Tuple2::_1);
    }

    public Future<JsValue> post(String path, JsValue body, Map<String, String> query) {
        return post(path, Option.of(Json.stringify(body)), query);
    }

    public Future<JsValue> post(String path, JsValue body) {
        return post(path, Option.of(Json.stringify(body)), HashMap.empty());
    }

    public Future<JsValue> post(String path) {
        return post(path, Option.none(), HashMap.empty());
    }

    public Future<JsValue> post(String path, Map<String, String> query) {
        return post(path, Option.none(), query);
    }

    public Future<JsValue> post(String path, String body, Map<String, String> query) {
        return post(path, Option.of(body), query);
    }

    public Future<JsValue> post(String path, Option<String> body, Map<String, String> query) {
        return request(path, "POST", body, query).map(Tuple2::_1);
    }

    public Future<JsValue> put(String path, JsValue body) {
        return put(path, Option.of(Json.stringify(body)), HashMap.empty());
    }

    public Future<JsValue> put(String path, JsValue body, Map<String, String> query) {
        return put(path, Option.of(Json.stringify(body)), query);
    }

    public Future<JsValue> put(String path) {
        return put(path, Option.none(), HashMap.empty());
    }

    public Future<JsValue> put(String path, Map<String, String> query) {
        return put(path, Option.none(), query);
    }

    public Future<JsValue> put(String path, String body, Map<String, String> query) {
        return put(path, Option.of(body), query);
    }

    public Future<JsValue> put(String path, Option<String> body, Map<String, String> query) {
        return request(path, "PUT", body, query).map(Tuple2::_1);
    }

    public Future<Tuple2<JsValue, Response>> request(String path, String verb, Option<String> body, Map<String, String> query) {
        Future<Response> response = rawRequest(path, verb, body, query);

        //@formatter:off
        return response
                .flatMap(this::readEntityAsString)
                .map(pair -> pair.map1(json -> {
                            if(json == null) {
                                return null;
                            } else {
                                return Json.parse(json);
                            }
                        })
                );
        //@formatter:on
    }

    private Future<Tuple2<String, Response>> readEntityAsString(Response r) {
        HttpEntity entity = r.getEntity();
        if(entity == null) {
            return Future.successful(null);
        } else {
            return Try.of(() -> EntityUtils.toString(entity))
                    .map(s -> Tuple.of(s, r))
                    .map(Future::successful)
                    .getOrElseGet(Future::failed);
        }
    }

    public Future<Response> rawRequest(String path, String verb, Option<String> body, Map<String, String> query) {
        return body.map(b -> {
            HttpEntity entity = new NStringEntity(b, ContentType.APPLICATION_JSON);
            EsResponseListener esResponseListener = new EsResponseListener();
            restClient.performRequestAsync(verb, path, query.toJavaMap(), entity, esResponseListener);
            return esResponseListener.future();
        }).getOrElse(() -> {
            EsResponseListener esResponseListener = new EsResponseListener();
            restClient.performRequestAsync(verb, path, query.toJavaMap(), esResponseListener);
            return esResponseListener.future();
        });
    }

    private <T> Function1<T, T> identity() {
        return e -> e;
    }


    private static class EsResponseListener implements ResponseListener {

        public final Promise<Response> promise;

        public EsResponseListener() {
            promise = Promise.make();
        }

        public Future<Response> future() {
            return promise.future();
        }

        @Override
        public void onSuccess(Response response) {
            promise.success(response);
        }

        @Override
        public void onFailure(Exception exception) {
            promise.failure(exception);
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

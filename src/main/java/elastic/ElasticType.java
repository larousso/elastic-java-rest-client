package elastic;


import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.concurrent.Future;
import org.elasticsearch.client.Response;
import org.reactivecouchbase.json.JsValue;

import akka.NotUsed;
import akka.japi.function.Predicate;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import elastic.request.BulkItem;
import elastic.response.BulkResponse;
import elastic.response.IndexResponse;
import elastic.response.SearchResponse;
import io.vavr.Tuple2;
import io.vavr.control.Either;
import io.vavr.control.Option;
import io.vavr.control.Try;
import scala.concurrent.duration.FiniteDuration;

public class ElasticType {

    private final Elastic elastic;

    private final String index;

    ElasticType(Elastic elastic, String index) {
        this.elastic = elastic;
        this.index = index;
    }

    public String getIndexName() {
        return index;
    }

    public Future<SearchResponse> search(JsValue query) {
        return elastic.search(index, query);
    }

    public Future<IndexResponse> index(JsValue data, Option<String> mayBeId) {
        return elastic.index(index, data, mayBeId);
    }

    public Future<IndexResponse> index(JsValue data, Option<String> mayBeId, Boolean create, String parent, Boolean refresh) {
        return elastic.index(index, data, mayBeId, create, parent, refresh);
    }

    public Future<JsValue> delete(String id) {
        return elastic.delete(index, id);
    }

    public Future<JsValue> getIndex() {
        return elastic.getIndex(index);
    }

    public Future<JsValue> getMapping() {
        return elastic.getMapping(index);
    }

    public Future<JsValue> getSettings() {
        return elastic.getSettings(index);
    }

    public Future<JsValue> updateSettings(JsValue settings) {
        return elastic.updateSettings(index, settings);
    }

    public Future<JsValue> getAliases() {
        return elastic.getAliases(index);
    }

    public Future<JsValue> addAlias(String name) {
        return elastic.addAlias(index, name);
    }

    public Future<JsValue> deleteAlias(String name) {
        return elastic.addAlias(index, name);
    }

    public Future<JsValue> health() {
        return elastic.health(List.of(index), HashMap.empty());
    }

    public Future<JsValue> health(Map<String, String> querys) {
        return elastic.health(List.of(index), querys);
    }

    public Future<Long> count() {
        return elastic.count(index);
    }

    public Future<JsValue> refresh() {
        return elastic.refresh(index);
    }

    public Future<JsValue> forceMerge() {
        return elastic.forceMerge(index);
    }

    public Source<SearchResponse, NotUsed> scroll(JsValue query, String scrollTime) {
        return elastic.scroll(index, query, scrollTime);
    }

    public Future<Tuple2<Try<BulkResponse>, Response>> oneBulk(java.util.List<BulkItem> items) {
        return elastic.oneBulk(index, items);
    }

    public Flow<BulkItem, BulkResponse, NotUsed> bulk(Integer batchSize, Integer parallelisation) {
        return elastic.bulk(index, batchSize, null, parallelisation);
    }

    public Flow<BulkItem, BulkResponse, NotUsed> bulk(Integer batchSize, FiniteDuration within, Integer parallelisation) {
        return elastic.bulk(index, batchSize, within, parallelisation);
    }

    public Source<Either<Elastic.BulkFailure, BulkResponse>, NotUsed> oneBulkWithRetry(java.util.List<BulkItem> items, Integer nbRetry, FiniteDuration latency, Elastic.RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        return elastic.oneBulkWithRetry(index, items, nbRetry, latency, retryMode, isError);
    }

    public Flow<BulkItem, Either<Elastic.BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(Integer batchSize, FiniteDuration within, Integer parallelism, Integer nbRetry, FiniteDuration latency, Elastic.RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        return elastic.bulkWithRetry(index, batchSize, within, parallelism, nbRetry, latency, retryMode, isError);
    }

    public Flow<BulkItem, Either<Elastic.BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(String index, Integer batchSize, FiniteDuration within, Integer parallelism, Integer nbRetry, FiniteDuration latency, Elastic.RetryMode retryMode) {
        return elastic.bulkWithRetry(index, batchSize, within, parallelism, nbRetry, latency, retryMode);
    }

    public Flow<BulkItem, Either<Elastic.BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(Integer batchSize, Integer parallelism, Integer nbRetry, FiniteDuration latency, Elastic.RetryMode retryMode) {
        return elastic.bulkWithRetry(index, batchSize, null, parallelism, nbRetry, latency, retryMode);
    }

    public Flow<BulkItem, Either<Elastic.BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(Integer batchSize, Integer parallelism, Integer nbRetry, FiniteDuration latency, Elastic.RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        return elastic.bulkWithRetry(index, batchSize, null, parallelism, nbRetry, latency, retryMode, isError);
    }

    public Future<JsValue> deleteIndex() {
        return elastic.deleteIndex(index);
    }

    public Elastic getElastic() {
        return elastic;
    }
}

package elastic;

import java.util.concurrent.CompletionStage;

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
import javaslang.Tuple2;
import javaslang.control.Either;
import javaslang.control.Option;
import javaslang.control.Try;
import scala.concurrent.duration.FiniteDuration;

/**
 * Created by 97306p on 05/05/2017.
 */
public class Type {

    private final Elastic elastic;

    private final String index;

    private final String type;

    Type(Elastic elastic, String index, String type) {
        this.elastic = elastic;
        this.index = index;
        this.type = type;
    }

    public CompletionStage<SearchResponse> search(JsValue query) {
        return elastic.search(index, type, query);
    }

    public CompletionStage<IndexResponse> index(String index, String type, JsValue data, Option<String> mayBeId) {
        return elastic.index(index, type, data, mayBeId);
    }

    public CompletionStage<IndexResponse> index(JsValue data, Option<String> mayBeId, Boolean create, String parent, Boolean refresh) {
        return elastic.index(index, type, data, mayBeId, create, parent, refresh);
    }

    public CompletionStage<JsValue> getIndex() {
        return elastic.getIndex(index);
    }

    public CompletionStage<JsValue> getMapping() {
        return elastic.getMapping(index, type);
    }

    public CompletionStage<Long> count() {
        return elastic.count(index, type);
    }

    public CompletionStage<JsValue> refresh() {
        return elastic.refresh(index);
    }

    public CompletionStage<Tuple2<Try<BulkResponse>, Response>> oneBulk(java.util.List<BulkItem> items) {
        return elastic.oneBulk(index, type, items);
    }

    public Flow<BulkItem, BulkResponse, NotUsed> bulk(Integer batchSize, Integer parallelisation) {
        return elastic.bulk(index, type, batchSize, null, parallelisation);
    }

    public Flow<BulkItem, BulkResponse, NotUsed> bulk(Integer batchSize, FiniteDuration within, Integer parallelisation) {
       return elastic.bulk(index, type, batchSize, within, parallelisation);
    }

    public Source<Either<Elastic.BulkFailure, BulkResponse>, NotUsed> oneBulkWithRetry(java.util.List<BulkItem> items, Integer nbRetry, FiniteDuration latency, Elastic.RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        return elastic.oneBulkWithRetry(index, type, items, nbRetry, latency, retryMode, isError);
    }

    public Flow<BulkItem, Either<Elastic.BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(Integer batchSize, FiniteDuration within, Integer parallelism, Integer nbRetry, FiniteDuration latency, Elastic.RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        return elastic.bulkWithRetry(index, type, batchSize, within, parallelism, nbRetry, latency, retryMode, isError);
    }

    public Flow<BulkItem, Either<Elastic.BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(String index, String type, Integer batchSize, FiniteDuration within, Integer parallelism, Integer nbRetry, FiniteDuration latency, Elastic.RetryMode retryMode) {
        return elastic.bulkWithRetry(index, type, batchSize, within, parallelism, nbRetry, latency, retryMode);
    }

    public Flow<BulkItem, Either<Elastic.BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(Integer batchSize, Integer parallelism, Integer nbRetry, FiniteDuration latency, Elastic.RetryMode retryMode) {
        return elastic.bulkWithRetry(index, type, batchSize, null, parallelism, nbRetry, latency, retryMode);
    }

    public Flow<BulkItem, Either<Elastic.BulkFailure, BulkResponse>, NotUsed> bulkWithRetry(Integer batchSize, Integer parallelism, Integer nbRetry, FiniteDuration latency, Elastic.RetryMode retryMode, Predicate<Tuple2<Try<BulkResponse>, Response>> isError) {
        return elastic.bulkWithRetry(index, type, batchSize, null, parallelism, nbRetry, latency, retryMode, isError);
    }


}

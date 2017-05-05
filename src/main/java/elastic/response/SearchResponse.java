package elastic.response;

import javaslang.control.Option;
import org.reactivecouchbase.json.*;
import org.reactivecouchbase.json.mapping.JsResult;
import org.reactivecouchbase.json.mapping.Reader;
import static org.reactivecouchbase.json.Syntax.*;

import javaslang.collection.List;
import org.reactivecouchbase.json.mapping.Writer;

public class SearchResponse {

    public static final Reader<SearchResponse> reads = json -> {
        try {
            return JsResult.success(new SearchResponse(
                    json.field("took").asOptInteger().getOrElse(() -> null),
                    json.field("timed_out").asOptBoolean().getOrElse(() -> null),
                    json.field("_shards"),
                    json.field("max_score").asOptInteger().getOrElse(() -> null),
                    json.field("_scroll_id").asOptString().getOrElse(() -> null),
                    json.field("hits").as(Hits.reads),
                    json.field("aggregations"),
                    json.field("acknowledged").asOptBoolean().getOrElse(() -> null),
                    json.field("status").asOptInteger().getOrElse(() -> null),
                    json.field("error")
            ));
        } catch (Exception e) {
            return JsResult.error(e);
        }
    };

    public static final Writer<SearchResponse> writes = response -> {
        List<JsPair> jsPairs = List.of(
                Option.of(response.took).map(n -> $("took", n)),
                Option.of(response._scroll_id).map(n -> $("_scroll_id", n)),
                Option.of(response.timed_out).map(n -> $("timed_out", n)),
                Option.of(response._shards).map(n -> $("_shards", n)),
                Option.of(response.max_score).map(n -> $("max_score", n)),
                Option.of($("hits", Json.toJson(response.hits, Hits.writes))),
                Option.of(response.aggregations).map(n -> $("aggregations", n)),
                Option.of(response.acknowledged).map(n -> $("acknowledged", n)),
                Option.of(response.status).map(n -> $("status", n)),
                Option.of(response.error).map(n -> $("error", n))
        ).flatMap(e -> e);
        return Json.obj(jsPairs.toJavaArray(JsPair.class));
    };



    public final Integer took;

    public final Boolean timed_out;

    public final JsValue _shards;

    public final Integer max_score;

    public final String _scroll_id;

    public final Hits hits;

    public final JsValue aggregations;

    public final Boolean acknowledged;

    public final Integer status;

    public final JsValue error;

    public SearchResponse(Integer took, Boolean timed_out, JsValue _shards, Integer max_score, String scroll_id, Hits hits, JsValue aggregations, Boolean acknowledged, Integer status, JsValue error) {
        this.took = took;
        this.timed_out = timed_out;
        this._shards = _shards;
        this.max_score = max_score;
        _scroll_id = scroll_id;
        this.hits = hits;
        this.aggregations = aggregations;
        this.acknowledged = acknowledged;
        this.status = status;
        this.error = error;
    }

    public JsValue error() {
        if(error == null) {
            return new JsNull();
        } else {
            return error;
        }
    }

    public static class Hits {

        public static final Reader<Hits> reads = json -> {
            try {
                return JsResult.success(new Hits(
                        json.field("total").asOptLong().getOrElse(() -> null),
                        json.field("max_score").asOptInteger().getOrElse(() -> null),
                        List.ofAll(json.field("hits").asArray())
                ));
            } catch (Exception e) {
                return JsResult.error(e);
            }
        };

        public static final Writer<Hits> writes = hits -> {
            List<JsPair> fields = List.of(
                Option.of(hits.total).map(n -> $("total", n)),
                Option.of(hits.max_score).map(n -> $("max_score", n)),
                Option.of($("hits", Json.arr(hits.hits.toJavaArray())))
            ).flatMap(e -> e);
            return Json.obj(fields.toJavaArray(JsPair.class));
        };

        public Long total;

        public Integer max_score;

        public List<JsValue> hits = List.empty();

        public Hits(Long total, Integer max_score, List<JsValue> hits) {
            this.total = total;
            this.max_score = max_score;
            this.hits = hits;
        }

        public <T> List<T> hitsAs(Reader<T> hitRead) {
            return hits
                    .map(json -> json.field("_source"))
                    .map(json -> Json.fromJson(json, hitRead).get());
        }

        public Long total() {
            return total;
        }

        public Integer maxScore() {
            return max_score;
        }

        public List<JsValue> hits() {
            return List.ofAll(hits);
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("SearchResponse{");
        sb.append("took=").append(took);
        sb.append(", timed_out=").append(timed_out);
        sb.append(", _shards=").append(_shards);
        sb.append(", hits=").append(hits);
        sb.append(", aggregations=").append(aggregations);
        sb.append(", acknowledged=").append(acknowledged);
        sb.append(", status=").append(status);
        sb.append(", error=").append(error);
        sb.append('}');
        return sb.toString();
    }
}

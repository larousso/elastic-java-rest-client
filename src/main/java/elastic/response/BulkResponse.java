package elastic.response;

import javaslang.control.Option;
import org.reactivecouchbase.json.*;
import org.reactivecouchbase.json.mapping.JsResult;
import org.reactivecouchbase.json.mapping.Reader;

import javaslang.collection.List;
import org.reactivecouchbase.json.mapping.Writer;

import javax.swing.*;

import static org.reactivecouchbase.json.Syntax.$;


public class BulkResponse {

    public static Reader<BulkResponse> reads = json -> {
        try {
            return JsResult.success(new BulkResponse(
                    json.field("took").asOptLong().getOrElse(() -> null),
                    json.field("errors").asOptBoolean().getOrElse(() -> null),
                    List.ofAll(json.field("items").asArray()).map(js -> js.read(BulkItem.reads).get())
            ));
        } catch (Exception e) {
            return JsResult.error(e);
        }
    };

    public static final Writer<BulkResponse> writes = response -> Json.obj(
            $("took", response.took),
            $("errors", response.errors),
            $("items", Json.arr(response.items.map(i -> Json.toJson(i, BulkItem.writes)).toJavaArray()))
    );

    public final Long took;
    public final Boolean errors;
    public final List<BulkItem> items;

    public BulkResponse(Long took, Boolean errors, List<BulkItem> items) {
        this.took = took;
        this.errors = errors;
        this.items = items == null ? List.empty() : items;
    }

    public List<BulkItem> getErrors() {
        return List.ofAll(items).filter(BulkItem::hasError);
    }

    public static class BulkItem {

        public static final Reader<BulkItem> reads =  json -> {
            try {
                return JsResult.success(new BulkItem(
                    json.field("_id").asOptString().getOrElse(() -> null),
                    json.field("_index").asOptString().getOrElse(() -> null),
                    json.field("_type").asOptString().getOrElse(() -> null),
                    json.field("index").asOptObject().map(o -> o.as(BulkResult.reads)).getOrElse(() -> null),
                    json.field("create").asOptObject().map(o -> o.as(BulkResult.reads)).getOrElse(() -> null),
                    json.field("update").asOptObject().map(o -> o.as(BulkResult.reads)).getOrElse(() -> null),
                    json.field("delete").asOptObject().map(o -> o.as(BulkResult.reads)).getOrElse(() -> null),
                    json.field("error"),
                    json.field("status"),
                    json.field("took").asOptLong().getOrElse(() -> null),
                    json.field("_version").asOptString().getOrElse(() -> null)
                ));
            } catch (Exception e) {
                return JsResult.error(e);
            }
        };

        public static final Writer<BulkItem> writes = item -> Json.obj(
                $("_id", item._id),
                $("_index", item._index),
                $("_type", item._type),
                $("index", Json.toJson(item.index, BulkResult.writes)),
                $("create", Json.toJson(item.index, BulkResult.writes)),
                $("update", Json.toJson(item.index, BulkResult.writes)),
                $("delete", Json.toJson(item.index, BulkResult.writes)),
                $("error", item.error),
                $("status", item.status),
                $("took", item.took),
                $("_version", item._version)
        );

        public final String _id;
        public final String _index;
        public final String _type;
        public final BulkResult index;
        public final BulkResult create;
        public final BulkResult update;
        public final BulkResult delete;
        public final JsValue error;
        public final JsValue status;
        public final Long took;
        public final String _version;

        public BulkItem(String _id, String _index, String _type, BulkResult index, BulkResult create, BulkResult update, BulkResult delete, JsValue error, JsValue status, Long took, String _version) {
            this._id = _id;
            this._index = _index;
            this._type = _type;
            this.index = index;
            this.create = create;
            this.update = update;
            this.delete = delete;
            this.error = error;
            this.status = status;
            this.took = took;
            this._version = _version;
        }

        public BulkResult bulkResult() {
            if(index != null)
                return index;
            if(create != null)
                return create;
            if(update != null)
                return update;
            if(delete != null)
                return delete;
            return null;
        }

        public Boolean hasError() {
            return bulkResult() == null ? false : bulkResult().hasError();
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("BulkItem{");
            sb.append("index=").append(index);
            sb.append(", create=").append(create);
            sb.append(", update=").append(update);
            sb.append(", delete=").append(delete);
            sb.append(", error=").append(error);
            sb.append(", status=").append(status);
            sb.append(", took=").append(took);
            sb.append('}');
            return sb.toString();
        }
    }

    public static class BulkResult {

        public static final Reader<BulkResult> reads =  json -> {
            try {
                return JsResult.success(new BulkResult(
                    json.field("_index").asOptString().getOrElse(() -> null),
                    json.field("_type").asOptString().getOrElse(() -> null),
                    json.field("_id").asOptString().getOrElse(() -> null),
                    json.field("_version").asOptInteger().getOrElse(() -> null),
                    json.field("status").asOptInteger().getOrElse(() -> null),
                    json.field("created").asOptBoolean().getOrElse(() -> null),
                    json.field("_shards"),
                    json.field("error")
                ));
            } catch (Exception e) {
                return JsResult.error(e);
            }
        };

        public static final Writer<BulkResult> writes = result -> Json.obj(List.of(
                Option.of(result._index).map(n -> $("_index", n)),
                Option.of(result._type).map(n -> $("_type", n)),
                Option.of(result._id).map(n -> $("_id", n)),
                Option.of(result._version).map(n -> $("_version", n)),
                Option.of(result.status).map(n -> $("status", n)),
                Option.of(result.created).map(n -> $("created", n)),
                Option.of(result._shards).map(n -> $("_shards", n)),
                Option.of($("error", result.error))
        ).flatMap(e -> e).toJavaArray(JsPair.class));

        public final String _index;
        public final String _type;
        public final String _id;
        public final Integer _version;
        public final Integer status;
        public final Boolean created;
        public final JsValue _shards;
        public final JsValue error;

        public BulkResult(String _index, String _type, String _id, Integer _version, Integer status, Boolean created, JsValue _shards, JsValue error) {
            this._index = _index;
            this._type = _type;
            this._id = _id;
            this._version = _version;
            this.status = status;
            this.created = created;
            this._shards = _shards;
            this.error = error;
        }

        public Boolean hasError() {
            return error() instanceof JsObject;
            //return !error().equals(new JsUndefined()) && !error().equals(new JsNull());
        }

        public JsValue error() {
            if(error == null) {
                return new JsNull();
            } else {
                return error;
            }
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("BulkResult{");
            sb.append("_index='").append(_index).append('\'');
            sb.append(", _type='").append(_type).append('\'');
            sb.append(", _id='").append(_id).append('\'');
            sb.append(", _version=").append(_version);
            sb.append(", status=").append(status);
            sb.append(", created=").append(created);
            sb.append(", _shards=").append(_shards);
            sb.append(", error=").append(error);
            sb.append('}');
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("BulkResponse{");
        sb.append("took=").append(took);
        sb.append(", errors=").append(errors);
        sb.append(", items=").append(items);
        sb.append('}');
        return sb.toString();
    }
}

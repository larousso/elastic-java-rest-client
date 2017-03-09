package elastic.response;

import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.mapping.JsResult;
import org.reactivecouchbase.json.mapping.Reader;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

public class IndexResponse {

    public static final Reader<IndexResponse> reads = json -> {
        try {
            return JsResult.success(new IndexResponse(
                    json.field("_index").asOptString().getOrElse((String)null),
                    json.field("_type").asOptString().getOrElse((String)null),
                    json.field("_id").asOptString().getOrElse((String)null),
                    json.field("_version").asInteger(),
                    json.field("created").asBoolean(),
                    json.field("found").asOptBoolean().getOrElse((Boolean) null),
                    json.field("_shards")
            ));
        } catch (Exception e) {
            return JsResult.error(e);
        }
    };

    public final String _index;

    public final String _type;

    public final String _id;

    public final Integer _version;

    public final Boolean created;

    public final Boolean found;

    public final JsValue _shards;

    public IndexResponse(String _index, String _type, String _id, Integer _version, Boolean created, Boolean found, JsValue _shards) {
        this._index = _index;
        this._type = _type;
        this._id = _id;
        this._version = _version;
        this.created = created;
        this.found = found;
        this._shards = _shards;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("IndexResponse{");
        sb.append("_shards=").append(_shards);
        sb.append(", _index='").append(_index).append('\'');
        sb.append(", _type='").append(_type).append('\'');
        sb.append(", _id='").append(_id).append('\'');
        sb.append(", _version=").append(_version);
        sb.append(", created=").append(created);
        sb.append(", found=").append(found);
        sb.append('}');
        return sb.toString();
    }
}

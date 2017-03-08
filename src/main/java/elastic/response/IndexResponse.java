package elastic.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.mapping.JsResult;
import org.reactivecouchbase.json.mapping.Reader;
@JsonIgnoreProperties(ignoreUnknown=true)
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
                    json.field("_shards").asObject()
            ));
        } catch (Exception e) {
            return JsResult.error(e);
        }
    };

    public String _index;

    public String _type;

    public String _id;

    public Integer _version;

    public Boolean created;

    public Boolean found;

    public JsValue _shards;

    public IndexResponse() {
    }

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

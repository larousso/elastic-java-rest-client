package elastic.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import elastic.javaslang.BulkItemList;
import javaslang.collection.List;
import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BulkResponse {

    public static Reader<BulkResponse> reads = Json.reads(BulkResponse.class);

    public Long took;
    public Boolean errors;
    @JsonDeserialize(using = BulkItemList.class)
    public List<BulkItem> items = List.empty();

    public BulkResponse() {
    }

    public BulkResponse(Long took, Boolean errors, List<BulkItem> items) {
        this.took = took;
        this.errors = errors;
        this.items = items;
    }

    public List<BulkItem> getErrors() {
        return List.ofAll(items).filter(BulkItem::hasError);
    }

    public static class BulkItem {
        public String _id;
        public String _index;
        public String _type;
        public BulkResult index;
        public BulkResult create;
        public BulkResult update;
        public BulkResult delete;
        public JsValue error;
        public JsValue status;
        public Long took;
        public String _version;

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

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BulkResult {
        public String _index;
        public String _type;
        public String _id;
        public Integer _version;
        public Integer status;
        public Boolean created;
        public JsValue _shards;
        public JsValue error;

        public Boolean hasError() {
            return error != null;
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

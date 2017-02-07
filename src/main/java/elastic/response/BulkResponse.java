package elastic.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Format;

import com.fasterxml.jackson.databind.JsonNode;

import javaslang.collection.List;


public class BulkResponse {

    public static Format<BulkResponse> format = Json.format(BulkResponse.class);

    public Long took;
    public Boolean errors;
    public java.util.List<BulkItem> items;

    public BulkResponse() {
    }

    public BulkResponse(Long took, Boolean errors, java.util.List<BulkItem> items) {
        this.took = took;
        this.errors = errors;
        this.items = items;
    }

    public java.util.List<BulkItem> getErrors() {
        return List.ofAll(items).filter(BulkItem::hasError).toJavaList();
    }

    public static class BulkItem {
        public BulkResult index;
        public BulkResult create;
        public BulkResult update;
        public BulkResult delete;
        public JsonNode error;
        public JsonNode status;
        public Long took;

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
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BulkResult {
        public String _index;
        public String _type;
        public String _id;
        public Integer _version;
        public Integer status;
        public Boolean created;
        public JsonNode _shards;
        public JsonNode error;

        public Boolean hasError() {
            return error != null;
        }

        public JsValue error() {
            if(error == null) {
                return new JsNull();
            } else {
                return Json.fromJsonNode(error);
            }
        }
    }

    public String stringify() {
        return Json.stringify(Json.toJson(this));
    }

    @Override
    public String toString() {
        return stringify();
    }
}

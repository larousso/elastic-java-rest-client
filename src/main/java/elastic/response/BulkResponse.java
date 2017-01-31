package elastic.response;

import com.fasterxml.jackson.databind.JsonNode;
import javaslang.collection.List;
import org.reactivecouchbase.json.JsNull;
import org.reactivecouchbase.json.JsValue;
import org.reactivecouchbase.json.Json;
import org.reactivecouchbase.json.mapping.Reader;


/**
 * Created by adelegue on 20/10/2016.
 */
public class BulkResponse {

    public static Reader<BulkResponse> reader = Json.reads(BulkResponse.class);

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

    public List<BulkItem> getErrors() {
        return List.ofAll(items).filter(BulkItem::hasError);
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


    public static class BulkResult {
        public String _index;
        public String _type;
        public String _id;
        public Integer _version;
        public Integer status;
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
}

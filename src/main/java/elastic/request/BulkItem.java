package elastic.request;

import org.reactivecouchbase.json.JsValue;

import com.fasterxml.jackson.annotation.JsonInclude;

public class BulkItem {

    public final BulkOperation operation;
    public final JsValue source;

    private BulkItem(BulkOperation operation, JsValue source) {
        this.operation = operation;
        this.source = source;
    }

    public static BulkItem item(BulkOperation operation, JsValue source) {
        return new BulkItem(operation, source);
    }

    public static BulkItem index(String _index, String _id, JsValue source) {
        return item(BulkOperation.index(_index, _id), source);
    }

    public static BulkItem index(String _id, JsValue source) {
        return item(BulkOperation.index(_id), source);
    }

    public static BulkItem create(String _index, String _id, JsValue source) {
        return item(BulkOperation.create(_index, _id), source);
    }

    public static BulkItem create(String _id, JsValue source) {
        return item(BulkOperation.create(_id), source);
    }

    public static BulkItem update(String _index, String _id, JsValue source) {
        return item(BulkOperation.update(_index, _id), source);
    }

    public static BulkItem update(String _id, JsValue source) {
        return item(BulkOperation.update(_id), source);
    }

    public static BulkItem delete(String _index, String _id) {
        return item(BulkOperation.delete(_index, _id), null);
    }

    public static BulkItem delete(String _id) {
        return item(BulkOperation.delete(_id), null);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class BulkOperation {
        public final BulkOperationDetail index;
        public final BulkOperationDetail create;
        public final BulkOperationDetail update;
        public final BulkOperationDetail delete;

        public static BulkOperation index(String _index, String _id) {
            return new BulkOperation(new BulkOperationDetail(_index, _id), null, null, null);
        }

        public static BulkOperation index(String _id) {
            return new BulkOperation(new BulkOperationDetail(null, _id), null, null, null);
        }

        public static BulkOperation create(String _index, String _id) {
            return new BulkOperation(null, new BulkOperationDetail(_index, _id), null, null);
        }

        public static BulkOperation create(String _id) {
            return new BulkOperation(null, new BulkOperationDetail(null, _id), null, null);
        }

        public static BulkOperation update(String _index, String _id) {
            return new BulkOperation(null, null, new BulkOperationDetail(_index, _id), null);
        }

        public static BulkOperation update(String _id) {
            return new BulkOperation(null, null, new BulkOperationDetail(null, _id), null);
        }

        public static BulkOperation delete(String _index, String _id) {
            return new BulkOperation(null, null, null, new BulkOperationDetail(_index, _id));
        }

        public static BulkOperation delete(String _id) {
            return new BulkOperation(null, null, null, new BulkOperationDetail(null, _id));
        }

        private BulkOperation(BulkOperationDetail index, BulkOperationDetail create, BulkOperationDetail update, BulkOperationDetail delete) {
            this.index = index;
            this.create = create;
            this.update = update;
            this.delete = delete;
        }
    }
    public static class BulkOperationDetail {
        public final String _index;
        public final String _id;

        public BulkOperationDetail(String _index, String _id) {
            this._index = _index;
            this._id = _id;
        }
    }
}

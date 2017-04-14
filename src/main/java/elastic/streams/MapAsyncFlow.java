package elastic.streams;

import akka.japi.function.Function;
import akka.japi.function.Procedure;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.*;
import javaslang.control.Option;

import java.util.concurrent.CompletionStage;

/**
 * Created by adelegue on 14/04/2017.
 */
public class MapAsyncFlow<I, O> extends GraphStage<FlowShape<I, O>> {

    public final Inlet<I> in = Inlet.create("MapAsyncFlow.in");
    public final Outlet<O> out = Outlet.create("MapAsyncFlow.out");

    private final FlowShape<I, O> shape = FlowShape.of(in, out);

    private final Function<I, CompletionStage<O>> f;

    public MapAsyncFlow(Function<I, CompletionStage<O>> f) {
        this.f = f;
    }


    @Override
    public FlowShape<I, O> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape) {

            Option<O> fValue = Option.none();
            Boolean isCalled = false;

            AsyncCallback<O> successCb = createAsyncCallback(param -> {
                    if(isAvailable(out)) {
                        push(out, param);
                    } else {
                        fValue = Option.some(param);
                    }
                    isCalled = false;
            });

            AsyncCallback<Throwable> failureCb = createAsyncCallback(e ->
                    fail(out, e)
            );
            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        isCalled = true;
                        f.apply(grab(in)).whenComplete((ok, e) -> {
                            if(e != null) {
                                failureCb.invoke(e);
                            } else {
                                successCb.invoke(ok);
                            }
                        });
                    }
                });
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        fValue.forEach(e -> {
                            pull(in);
                            push(out, e);
                        });

                    }
                });
            }
        };
    }

}


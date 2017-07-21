package elastic.streams;

import akka.japi.function.Function;
import akka.japi.function.Procedure;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.*;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.control.Option;

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

            List<I> buffer = List.empty();
            Option<O> fValue = Option.none();
            Boolean completeStream = false;

            AsyncCallback<O> successCb = createAsyncCallback(param -> {
                if(isAvailable(out)) {
                    push(out, param);
                } else {
                    fValue = Option.some(param);
                }
                if(completeStream) {
                    complete(out);
                }
                call();
            });

            AsyncCallback<Throwable> failureCb = createAsyncCallback(e ->{
                e.printStackTrace();
                fail(out, e);
            });

            private void call() throws Exception {
                if(!buffer.isEmpty()) {
                    Tuple2<I, List<I>> pop = buffer.pop2();
                    buffer = pop._2;
                    f.apply(pop._1).whenComplete((ok, e) -> {
                        if(e != null) {
                            failureCb.invoke(e);
                        } else {
                            successCb.invoke(ok);
                        }
                    });
                }
            }

            private void pushTmp() {
                fValue.forEach(v -> {
                        push(out, v);
                });
            }

            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        I grab = grab(in);
                        buffer = buffer.append(grab);
                        call();
                    }

                    @Override
                    public void onUpstreamFinish() throws Exception {
                        completeStream = true;
                        call();
                    }
                });
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        if(!isClosed(in)) {
                            pull(in);
                        }
                        call();
                        pushTmp();
                    }
                });
            }

        };
    }

}


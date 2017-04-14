package elastic.streams;

import akka.NotUsed;
import akka.japi.function.Function;
import akka.stream.javadsl.Flow;

import java.util.concurrent.CompletionStage;

/**
 * Created by 97306p on 14/04/2017.
 */
public class Flows {

    public static <I, O> Flow<I, O, NotUsed> mapAsync(Function<I, CompletionStage<O>> f) {
        return Flow.fromGraph(new MapAsyncFlow<>(f));
    }

}

package org.kot.test.etl.akka.stream;

import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.IOResult;
import akka.stream.SinkShape;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Sink;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Graph utility.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2018-12-15 23:20
 */
public interface Grapher {

	default <I, O> Graph<FlowShape<I, O>, ?> balance(int numberOfWorkers, Flow<I, O, ?> worker) {
		return GraphDSL.create(builder -> {
			UniformFanOutShape<I, I> balance = builder.add(Balance.create(numberOfWorkers));
			UniformFanInShape<O, O> merge = builder.add(Merge.create(numberOfWorkers));

			for (int i = 0; i < numberOfWorkers; i++) {
				FlowShape<I, O> workerStage = builder.add(worker);
				builder.from(balance.out(i))
						.via(workerStage)
						.toInlet(merge.in(i));
			}

			return FlowShape.of(balance.in(), merge.out());
		});
	}

	default  <I> Graph<SinkShape<I>, CompletionStage<IOResult>> sinkTo(Sink<I, CompletionStage<IOResult>> outSink,
	                                                                        Sink<I, CompletionStage<IOResult>> errSink) {
		return GraphDSL.create(
				outSink, errSink, Keep.left(), (builder, out, err) -> {
					final UniformFanOutShape<I, I> broadcast = builder.add(Broadcast.create(2));

					builder.from(broadcast.out(0))
							.to(out)
							.from(broadcast.out(1))
							.to(err);
					return SinkShape.of(broadcast.in());
				});

	}

	default <I> Graph<SinkShape<I>, List<CompletionStage<IOResult>>> sinkTo(List<Sink<I, CompletionStage<IOResult>>> tails) {
		return GraphDSL.create(
				tails, (builder, list) -> {
					final UniformFanOutShape<I, I> broadcast = builder.add(Broadcast.create(list.size()));

					for (int t = 0; t < list.size(); t++) {
						builder.from(broadcast.out(t))
								.to((SinkShape) list.get(t));
					}
					return SinkShape.of(broadcast.in());
				});

	}
}

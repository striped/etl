package org.kot.test.etl.akka.stream;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.Shape;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.InHandler;
import akka.util.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Graph stage in flow shape to do a binding from Java to CSV.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2018-12-16 16:20
 */
public class Java2CSVStage extends GraphStage<FlowShape<Map<String, ?>, ByteString>> {

	private final Inlet<Map<String, ?>> in;

	private final Outlet<ByteString> out;

	private final FlowShape<Map<String, ?>, ByteString> shape;

	private final ByteString header;

	private final List<Binding.Mapping<?, ByteString>> adapters;

	private Java2CSVStage(List<Binding.Mapping<?, ByteString>> mappings) {
		in = Inlet.create("Java2CSV.in");
		out = Outlet.create("Java2CSV.out");
		shape = FlowShape.of(in, out);
		header = ByteString.fromString(mappings.stream().map(Binding.Mapping::toName).collect(Collectors.joining(",")) + "\n");
		adapters = mappings;
	}

	@SuppressWarnings("WeakerAccess")
	public static Java2CSVStage toMapping(List<Binding.Mapping<?, ByteString>> mappings) {
		return new Java2CSVStage(mappings);
	}

	@Override
	public FlowShape<Map<String, ?>, ByteString> shape() {
		return shape;
	}

	@Override
	public GraphStageLogic createLogic(Attributes inheritedAttributes) {
		return new Logic(shape);
	}

	private ByteString toLine(Map<String, ?> map) {
		List<String> errors = new ArrayList<>();
		ByteString delimiter = ByteString.fromString(",");

		return adapters.stream()
				.map(a -> {
					Object v = map.get(a.toName());
					return ((Binding.Mapping<Object, ByteString>) a).apply(v, errors);
				})
				.reduce(ByteString.empty(), (v, u) -> v.isEmpty()? u: v.concat(delimiter).concat(u))
				.concat(ByteString.fromString("\n"));
	}

	private class Logic extends GraphStageLogic {

		private final InHandler rest = new AbstractInHandler() {
			@Override
			public void onPush() {
				Map<String, ?> item = grab(in);
				push(out, toLine(item));
			}
		};

		Logic(Shape shape) {
			super(shape);
			setHandler(in, new AbstractInHandler() {
				@Override
				public void onPush() {
					Map<String, ?> item = grab(in);
					push(out, header.concat(toLine(item)));
					setHandler(in, rest);
				}
			});
			setHandler(out, new AbstractOutHandler() {
				@Override
				public void onPull() {
					pull(in);
				}
			});
		}

	}
}

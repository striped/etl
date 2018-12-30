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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Graph stage in flow shape to do a binding from CSV to Java.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2018-12-16 16:20
 */
public class CSV2JavaStage extends GraphStage<FlowShape<List<ByteString>, Map<String, ?>>> {

	private final Inlet<List<ByteString>> in;

	private final Outlet<Map<String, ?>> out;

	private final FlowShape<List<ByteString>, Map<String, ?>> shape;

	private final List<ByteString> header;

	private final List<Binding.Mapping<ByteString, ?>> adapters;

	private CSV2JavaStage(List<Binding.Mapping<ByteString, ?>> mappings) {
		in = Inlet.create("CSV2Java.in");
		out = Outlet.create("CSV2Java.out");
		shape = FlowShape.of(in, out);
		header = mappings.stream().map(Binding.Mapping::fromName).map(ByteString::fromString).collect(Collectors.toList());
		adapters = mappings;
	}

	@SuppressWarnings("WeakerAccess")
	public static CSV2JavaStage toMappings(List<Binding.Mapping<ByteString, ?>> mappings) {
		return new CSV2JavaStage(mappings);
	}

	@Override
	public FlowShape<List<ByteString>, Map<String, ?>> shape() {
		return shape;
	}

	@Override
	public GraphStageLogic createLogic(Attributes inheritedAttributes) {
		return new Logic(shape);
	}

	private Map<String, Object> toMap(Iterator<ByteString> cells, BiFunction<Binding.Mapping<ByteString, ?>, ByteString, Object> converter) {
		Map<String, Object> map = new HashMap<>();
		adapters.forEach(a -> {
			ByteString value = cells.hasNext()? cells.next(): null;
			Object result = converter.apply(a, value);
			if (null != result) {
				map.put(a.toName(), result);
			}
		});
		return map;
	}

	private class Logic extends GraphStageLogic {

		private final InHandler rest = new AbstractInHandler() {
			@Override
			public void onPush() {
				List<ByteString> row = grab(in);

				List<String> errors = new ArrayList<>();
				Map<String, Object> result = toMap(row.iterator(), (a, v) -> a.apply(v, errors));

				if (!errors.isEmpty()) {
					result = toMap(row.iterator(), (a, v) -> v.utf8String());
					result.put("failures", errors);
				}

				push(out, result);
			}
		};

		Logic(Shape shape) {
			super(shape);
			setHandler(in, new AbstractInHandler() {
				@Override
				public void onPush() {
					List<ByteString> row = grab(in);
					if (header.equals(row)) {
						setHandler(in, rest);
					}
					pull(in);
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

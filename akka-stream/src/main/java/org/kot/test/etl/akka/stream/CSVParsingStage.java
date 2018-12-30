package org.kot.test.etl.akka.stream;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.InHandler;
import akka.stream.stage.OutHandler;
import akka.util.ByteString;

import java.io.IOException;
import java.util.List;

/**
 * Transcription of Alpakka CSV parsing stage.
 * <a href="https://github.com/akka/alpakka/blob/master/csv/src/main/scala/akka/stream/alpakka/csv/impl/CsvParsingStage.scala">Alpakka CSV</a>.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2018-12-16 22:52
 */
public class CSVParsingStage extends GraphStage<FlowShape<ByteString, List<ByteString>>> {

	private final char delimiter;

	private final char quoteChar;

	private final char escapeChar;

	private final int maximumLineLength;

	private final Inlet<ByteString> in;
	private final Outlet<List<ByteString>> out;
	private final FlowShape<ByteString, List<ByteString>> shape;

	private CSVParsingStage(char delimiter,
	                        char quoteChar,
	                        char escapeChar,
	                        int maximumLineLength) {
		this.delimiter = delimiter;
		this.quoteChar = quoteChar;
		this.escapeChar = escapeChar;
		this.maximumLineLength = maximumLineLength;
		in = Inlet.create("CSVParsingStage.in");
		out = Outlet.create("CSVParsingStage.out");
		shape = FlowShape.of(in, out);
	}

	@SuppressWarnings("WeakerAccess")
	public static CSVParsingStage parse(char delimiter, char quoteChar, char escapeChar, int maximumLineLength) {
		return new CSVParsingStage(delimiter, quoteChar, escapeChar, maximumLineLength);
	}

	@Override
	public GraphStageLogic createLogic(Attributes inheritedAttributes) {
		return new CvsGraphStageLogic(shape);
	}

	@Override
	public FlowShape<ByteString, List<ByteString>> shape() {
		return shape;
	}

	private class CvsGraphStageLogic extends GraphStageLogic implements InHandler, OutHandler {

		private final CSVParser buffer;

		CvsGraphStageLogic(FlowShape<ByteString, List<ByteString>> shape) {
			super(shape);
			buffer = new CSVParser(delimiter, quoteChar, escapeChar, maximumLineLength);
			setHandlers(in, out, this);
		}

		@Override
		public void onPush() throws IOException {
			buffer.offer(grab(in));
			tryPollBuffer();
		}

		@Override
		public void onPull() {
			tryPollBuffer();
		}

		@Override
		public void onUpstreamFinish() {
			emitRemaining();
			completeStage();
		}

		private void tryPollBuffer() {
			List<ByteString> line = buffer.poll(true);
			if (null != line) {
				push(out, line);
			} else if (!isClosed(in)) {
				pull(in);
			} else {
				emitRemaining();
				completeStage();
			}
		}

		private void emitRemaining() {
			for (List<ByteString> line = buffer.poll(false); null != line; line = buffer.poll(false)) {
				emit(out, line);
			}
		}
	}
}

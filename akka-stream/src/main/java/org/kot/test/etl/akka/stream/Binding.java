package org.kot.test.etl.akka.stream;

import akka.util.ByteString;

import java.math.BigDecimal;
import java.text.ParsePosition;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * CSV to Java and vise versa binding rules.
 * Defines the data structure and rules how it binding when CSV is processed.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2018-12-15 22:35
 */
@SuppressWarnings("WeakerAccess")
public class Binding {

	public static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("d/M/uuuu").withResolverStyle(ResolverStyle.STRICT);

	private static final List<Mapping<ByteString, ?>> inMappings = csv2java().collect(Collectors.toList());

	private static final List<Mapping<?, ByteString>> errorMappings = error2csv().collect(Collectors.toList());

	private static final List<Mapping<?, ByteString>> outMappings = java2csv().collect(Collectors.toList());

	public static List<Mapping<ByteString, ?>> inMappings() {
		return inMappings;
	}

	public static List<Mapping<?, ByteString>> outMappings() {
		return outMappings;
	}

	public static List<Mapping<?, ByteString>> errorMappings() {
		return errorMappings;
	}

	private static Stream<Mapping<ByteString, ?>> csv2java() {
		Function<ByteString, LocalDate> toDate = v -> {
			if (null == v) {
				return null;
			}
			ParsePosition pos = new ParsePosition(0);
			TemporalAccessor value = DATE.parseUnresolved(v.utf8String(), pos);
			if (null == value) {
				return null;
			}
			try {
				return LocalDate.parse(v.utf8String(), DATE);
			} catch (Exception e) {
				return null;
			}
		};
		Function<ByteString, Integer> toPositiveInt = v -> {
			if (null == v || v.isEmpty()) {
				return null;
			}
			try {
				int result = Integer.parseInt(v.utf8String());
				return (0 > result)? null : result;
			} catch (NumberFormatException e) {
				return null;
			}
		};
		Function<ByteString, Integer> toInt = v -> {
			if (null == v || v.isEmpty()) {
				return null;
			}
			try {
				return Integer.parseInt(v.utf8String());
			} catch (NumberFormatException e) {
				return null;
			}
		};
		Function<ByteString, String> toString = v -> {
			if (null == v) {
				return null;
			}
			return v.utf8String();
		};
		Function<ByteString, BigDecimal> toDecimal = v -> {
			if (null == v || v.isEmpty()) {
				return null;
			}
			try {
				return new BigDecimal(v.utf8String());
			} catch (Exception e) {
				return null;
			}
		};
		return Stream.of(
				required("id", "id", toPositiveInt, v -> "Unable interpret '" + v + "' as ID"),
				mapping("col1", "col1", toString),
				required("col2","col2",  toDate, v -> "Unable interpret '" + v + "' as date for col2"),
				mapping("col3","col3",  toDecimal),
				mapping("col4","col4",  toInt)
		);
	}

	private static Stream<Mapping<?, ByteString>> java2csv() {

		Function<LocalDate, ByteString> fromDate = v -> {
			if (null == v) {
				return ByteString.empty();
			}
			return ByteString.fromString(DATE.format(v));
		};
		Function<Integer, ByteString> fromPositiveInt = v -> {
			if (null == v) {
				return ByteString.empty();
			}
			return ByteString.fromString(Integer.toString(v));
		};
		Function<Integer, ByteString> fromInt = v -> {
			if (null == v) {
				return ByteString.empty();
			}
			return ByteString.fromString(Integer.toString(v));
		};
		Function<String, ByteString> fromString = v -> {
			if (null == v) {
				return ByteString.empty();
			}
			return ByteString.fromString(v);
		};
		Function<BigDecimal, ByteString> fromDecimal = v -> {
			if (null == v) {
				return ByteString.empty();
			}
			return ByteString.fromString(v.toString());
		};
		return Stream.of(
				mapping("id", "id", fromPositiveInt),
				mapping("col1", "col1", fromString),
				mapping("col2","col2",  fromDate),
				mapping("col3","col3",  fromDecimal),
				mapping("col4","col4",  fromInt)
		);
	}

	private static Stream<Mapping<?, ByteString>> error2csv() {
		Function<String, ByteString> fromString = v -> {
			if (null == v) {
				return ByteString.empty();
			}
			return ByteString.fromString(v);
		};
		Function<List<String>, ByteString> fromList = (List<String> v) -> {
			if (null == v || v.isEmpty()) {
				return ByteString.empty();
			}

			String result = v.stream()
					.map(e -> e.replaceAll("\"", "\\\\\""))
					.collect(Collectors.joining(",", "\"", "\""));
			return ByteString.fromString(result);
		};
		return Stream.of(
				mapping("id", "id", fromString),
				mapping("col1", "col1", fromString),
				mapping("col2","col2",  fromString),
				mapping("col3","col3",  fromString),
				mapping("col4","col4",  fromString),
				mapping("failures", "failures", fromList)
		);
	}

	private static <I, O> Mapping mapping(String from, String to, Function<I, O> adapter) {
		return new Mapping<>(from, to, adapter);
	}

	private static <I, O> Mapping required(String from, String to, Function<I, O> adapter, Function<I, String> errorReporter) {
		return new RequiredMapping<>(from, to, adapter, errorReporter);
	}

	public static class Mapping<I, O> {

		private final String fromName;

		private final String toName;

		private final Function<I, O> adapter;

		Mapping(String fromName, String toName, Function<I, O> adapter) {
			this.fromName = fromName;
			this.toName = toName;
			this.adapter = adapter;
		}

		public String fromName() {
			return fromName;
		}

		public String toName() {
			return toName;
		}

		public O apply(I data, List<String> mismatch) {
			return adapter.apply(data);
		}

		@Override
		public String toString() {
			return "Mapping " + fromName + " -> " + toName;
		}
	}

	public static class RequiredMapping<I, O> extends Mapping<I, O> {

		private final Function<I, String> error;

		RequiredMapping(String fromName, String toName, Function<I, O> adapter, Function<I, String> error) {
			super(fromName, toName, adapter);
			this.error = error;
		}

		@Override
		public O apply(I data, List<String> mismatch) {
			O result = super.apply(data, mismatch);
			if (null == result) {
				mismatch.add(error.apply(data));
			}
			return result;
		}
	}
}

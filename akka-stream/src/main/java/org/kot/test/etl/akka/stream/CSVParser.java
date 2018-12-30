package org.kot.test.etl.akka.stream;

import akka.util.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Transcription of Alpakka CSV parser.
 * <a href="https://github.com/akka/alpakka/blob/master/csv/src/main/scala/akka/stream/alpakka/csv/impl/CsvParser.scala">Alpakka CSV</a>.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2018-12-18 22:35
 */
public class CSVParser implements Iterator<List<ByteString>> {

	private static final char CR = '\r', LF = '\n';

	private final int maxBufLength;

	private final char delimiter, quote, escape;

	private ByteString buf = ByteString.empty();

	private int line, a, b;

	private List<ByteString> next, rest;

	private char current;

	@SuppressWarnings("WeakerAccess")
	public CSVParser(char delimiter, char quoteChar, char escapeChar, int maximumLineLength) {
		this.delimiter = delimiter;
		this.quote = quoteChar;
		this.escape = escapeChar;
		this.maxBufLength = maximumLineLength;
		line = 1;
	}

	@SuppressWarnings("WeakerAccess")
	public void offer(ByteString part) throws IOException {
		if (0 < a) {
			buf = buf.slice(a, buf.size());
		}
		a = 0;
		if (maxBufLength < part.size() + buf.size()) {
			throw new IOException("Buffer exceed the limit " + maxBufLength + " bytes");
		}
		buf = buf.concat(part);
		next = calculateNext();
	}

	@SuppressWarnings("WeakerAccess")
	public List<ByteString> poll(boolean requireLineFeed) {
		if (hasNext()) {
			return next();
		} else if (!requireLineFeed && null != rest) {
			if (buf.size() > a) {
				rest.add(buf.slice(a, buf.length()));
			}
			List<ByteString> result = rest;
			rest = null;
			return result;
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		return null != next;
	}

	@Override
	public List<ByteString> next() {
		List<ByteString> result = next;
		next = calculateNext();
		return result;
	}

	private List<ByteString> calculateNext() {
		if (buf.size() <= a) {
			return null;
		}
		List<ByteString> result = (null != rest)? rest: new ArrayList<>();
		rest = null;
		for (ByteString field = nextField(); null != field; field = nextField()) {
			result.add(field);
			a = b + 1;
			if (buf.size() <= a) {
				break;
			}
			if (LF == current || CR == current) {
				line++;
				return result;
			}
			if (delimiter != current) {
				throw new IndexOutOfBoundsException("[" + line + "," + a + "]: Expected '" + delimiter + "' but got '" + current + "'");
			}
		}
		rest = result;
		return null; // failed find reasonable finish, may be need more data?
	}

	private ByteString nextField() {
		if (quote == buf.apply(a)) { // check quoted
			b = buf.indexOf(quote, a + 1);
			while (0 < b && escape == buf.apply(b - 1)) {
				b = buf.indexOf(quote, b + 1);
			}
			if (0 > b || buf.size() <= b + 1) {
				return null;
			}
			ByteString result = buf.slice(a, ++b);
			current = (char) buf.apply(b);
			return result;
		}
		b = findEdge(a);
		if (0 > b || buf.size() <= b) {
			return null; // fail to find end of field
		}
		current = (char) buf.apply(b);
		if (CR == current) {
			ByteString result = buf.slice(a, b);
			if (buf.size() > b && LF == buf.apply(b + 1)) {
				b += 1;
			}
			return result;
		}
		return buf.slice(a, b);
	}

	private int findEdge(int from) {
		for (int i = from; i < buf.size(); i++) {
			char c = (char) buf.apply(i);
			if (LF == c || delimiter == c || CR == c) {
				return i;
			}
		}
		return -1;
	}
}

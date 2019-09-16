package org.kot.mapping;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2019-07-14 23:49
 */
public class Converters {

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd/MM/uuuu")
			.withResolverStyle(ResolverStyle.STRICT);

	private Converters() {
	}

	public static LocalDate toDate(String v) {
		return LocalDate.parse(v, FORMATTER);
	}

	public static String fromDate(LocalDate v) {
		return FORMATTER.format(v);
	}

	public static BigDecimal toDecimal(String v) {
		return new BigDecimal(v);
	}
}

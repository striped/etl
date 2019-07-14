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

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("d/MM/yyyy")
			.withResolverStyle(ResolverStyle.STRICT);

	private Converters() {
	}

	public static LocalDate toDate(String v) {
		try {
			return LocalDate.parse(v, FORMATTER);
		} catch (Exception e) {
			return null;
		}
	}

	public static BigDecimal toDecimal(String v) {
		try {
			return new BigDecimal(v);
		} catch (Exception e) {
			return null;
		}
	}
}

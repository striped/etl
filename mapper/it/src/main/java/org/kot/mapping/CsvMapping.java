package org.kot.mapping;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2019-07-14 23:00
 */
@Mapping("mapping/csv.yaml")
public interface CsvMapping {

	Map<String, String> header();

	Map<String, Function<String, ?>> binders();

	static CsvMapping load() {
		Iterator<CsvMapping> i = ServiceLoader.load(CsvMapping.class)
				.iterator();
		return i.hasNext() ? i.next() : null;
	}

}

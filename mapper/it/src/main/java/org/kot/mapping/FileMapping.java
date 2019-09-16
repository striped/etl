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
@CSVMapping("mapping/csv.yaml")
public interface FileMapping {

	Map<String, String> header();

	Map<String, Function<String, ?>> binders();

	Map<String, Function<?, String>> serializers();

	static FileMapping load() {
		Iterator<FileMapping> i = ServiceLoader.load(FileMapping.class)
				.iterator();
		return i.hasNext() ? i.next() : null;
	}

}

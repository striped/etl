package org.kot.mapping;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2019-07-14 23:00
 */
@CSVMapping("mapping/csv.yaml")
public interface AdvFileReadOnlyMapping {

	Map<String, String> header();

	Map<String, Binder<String, ?>> binders();

	static AdvFileReadOnlyMapping load() {
		Iterator<AdvFileReadOnlyMapping> i = ServiceLoader.load(AdvFileReadOnlyMapping.class)
				.iterator();
		return i.hasNext() ? i.next() : null;
	}

	@FunctionalInterface
	interface Binder<I, O> {

		O bind(I value) throws Exception;
	}
}

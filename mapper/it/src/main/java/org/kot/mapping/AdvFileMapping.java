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
public interface AdvFileMapping {

	Map<String, String> header();

	Map<String, Binder<String, ?>> binders();

	Map<String, Binder<?, String>> serializers();

	@SuppressWarnings("unchecked")
	default <T> Binder<T, String> serializer(String id) {
		return (Binder<T, String>) serializers().get(id);
	}

	static AdvFileMapping load() {
		Iterator<AdvFileMapping> i = ServiceLoader.load(AdvFileMapping.class)
				.iterator();
		return i.hasNext() ? i.next() : null;
	}

	@FunctionalInterface
	interface Binder<I, O> {

		O bind(I value) throws Exception;
	}
}

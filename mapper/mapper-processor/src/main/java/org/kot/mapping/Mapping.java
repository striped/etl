package org.kot.mapping;

import java.util.List;
import java.util.stream.Stream;

/**
 * TODO Describe me.
 *
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 2019-08-08 19:15
 */
public class Mapping {

	private String defaultBinder;

	private String defaultSerializer;

	private List<Entry> entries;

	public String defaultBinder() {
		return defaultBinder;
	}

	public void setDefaultBinder(String binder) {
		this.defaultBinder = binder;
	}

	public String defaultSerializer() {
		return defaultSerializer;
	}

	public void setDefaultSerializer(String serializer) {
		this.defaultSerializer = serializer;
	}

	public Stream<Entry> entries() {
		return entries.stream().peek(e -> {
			if (null == e.serializer()) {
				e.setSerializer(defaultSerializer);
			}
			if (null == e.binder()) {
				e.setBinder(defaultBinder);
			}
		});
	}

	public void setEntries(List<Entry> entries) {
		this.entries = entries;
	}

	public static class Entry {

		private String id;

		private String name;

		private String binder;

		private String serializer;

		public String id() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String name() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String binder() {
			return binder;
		}

		public void setBinder(String binder) {
			this.binder = binder;
		}

		public String serializer() {
			return serializer;
		}

		public void setSerializer(String serializer) {
			this.serializer = serializer;
		}

		@Override
		public String toString() {
			return "" + id + " -> " + name;
		}
	}
}

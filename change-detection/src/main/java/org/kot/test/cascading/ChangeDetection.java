package org.kot.test.cascading;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Abstract assembly for all Change detection implementation.
 * @author <a href=mailto:striped@gmail.com>striped</a>
 * @created 08/04/15 21:03
 */
@SuppressWarnings("serial")
public abstract class ChangeDetection extends SubAssembly {

	protected final Fields fields;

	protected Pipe history;

	protected Pipe mainstream;

	protected Fields byKey;

	protected String sign;

	/**
	 * Constructs the assembly with expected fields declaration of data stream to work with.
	 * @param fields The fields declaration
	 */
	public ChangeDetection(Fields fields) {
		assert (fields != null) && fields.isDefined(): "Fields declaration expected";
		this.fields = fields;
	}

	/**
	 * Appends the "history" pipe with fields selector for a tuple key. This pipe imposes to share the same field declaration as a main stream.
	 * @param history The "history" pipe
	 * @return This builder instance
	 */
	public ChangeDetection the(Pipe history) {
		assert null != history: "History stream expected";
		this.history = history;
		return this;
	}

	/**
	 * Appends the "history" pipe with fields selector for a tuple key. This pipe imposes to share the same field declaration as a main stream.
	 * @param mainstream The main data stream
	 * @param byKey The tuple key fields, must be a part of {@link #ChangeDetection(cascading.tuple.Fields) main stream field declaration}
	 * @return This builder instance
	 */
	public ChangeDetection mergeWith(Pipe mainstream, Fields byKey) {
		assert null != mainstream: "Main data stream expected";
		this.mainstream = mainstream;
		assert fields.contains(byKey) : "Key selector expected beside stream field declaration";
		this.byKey = byKey;
		return this;
	}

	/**
	 * Instruct to use the {@code field} for a comparison.
	 * @param field The field name (beside {@link #ChangeDetection(cascading.tuple.Fields) main stream field declaration}) that need to compare to
	 * figure out data were changed or not.
	 * @return This builder instance
	 */
	public ChangeDetection compare(String field) {
		assert fields.contains(new Fields(field)) : "Field to compare expected beside stream field declaration";
		this.sign = field;
		return this;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}

package net.thumbtack.sharding.query;

import java.util.List;

public class QueryException extends RuntimeException {

	private static final long serialVersionUID = -6003272694020918874L;

	private List<QueryError> errors;

	public QueryException(String message, List<QueryError> errors) {
		super(message);
		this.errors = errors;
	}

	public List<QueryError> getErrors() {
		return errors;
	}
}

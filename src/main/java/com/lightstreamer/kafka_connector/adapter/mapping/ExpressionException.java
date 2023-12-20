package com.lightstreamer.kafka_connector.adapter.mapping;

public class ExpressionException extends RuntimeException {

    private final static long serialVersionUID = 1L;

    public ExpressionException(String message) {
        super(message);
    }

    public static ExpressionException throwBlankToken() {
        throw new ExpressionException("Tokens cannot be blank");
    }

    public static ExpressionException throwIncompleteExpression() {
        throw new ExpressionException("Incomplete expression");
    }

    public static ExpressionException throwExpectedToken(String token) {
        throw new ExpressionException("Expected <%s>".formatted(token));
    }

    public static ExpressionException throwInvalidExpression() {
        throw new ExpressionException("Invalid expression");
    }

}

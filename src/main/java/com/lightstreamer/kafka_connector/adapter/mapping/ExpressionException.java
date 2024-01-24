package com.lightstreamer.kafka_connector.adapter.mapping;

public class ExpressionException extends RuntimeException {

    private final static long serialVersionUID = 1L;

    public ExpressionException(String message) {
        super(message);
    }

    public ExpressionException(String message, Throwable cause) {
        super(message, cause);
    }

    public static void throwBlankToken() throws ExpressionException {
        throw new ExpressionException("Tokens cannot be blank");
    }

    public static void throwIncompleteExpression() throws ExpressionException {
        throw new ExpressionException("Incomplete expression");
    }

    public static void throwExpectedToken(String token) throws ExpressionException {
        throw new ExpressionException("Expected <%s>".formatted(token));
    }

    public static void throwInvalidExpression(String name, String expression) throws ExpressionException {
        throw new ExpressionException(
                "Found the invalid expression [%s] while evaluating [%s]".formatted(expression, name));
    }

    public static void reThrowInvalidExpression(ExpressionException cause, String name,
            String expression) throws ExpressionException {
        throw new ExpressionException("Found the invalid expression [%s] while evaluating [%s]: <%s>"
                .formatted(expression, name, cause.getMessage()));
    }
}

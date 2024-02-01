/* (C) 2024 */
package com.lightstreamer.kafka_connector.adapter.mapping;

public class ExpressionException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public ExpressionException(String message) {
    super(message);
  }

  public ExpressionException(String message, Throwable cause) {
    super(message, cause);
  }

  public static void throwBlankToken(String name, String expression) throws ExpressionException {
    throw new ExpressionException(
        "Found the invalid expression [%s] with missing tokens while evaluating [%s]"
            .formatted(expression, name));
  }

  public static void throwExpectedRootToken(String name, String token) throws ExpressionException {
    throw new ExpressionException(
        "Expected the root token [%s] while evaluating [%s]".formatted(token, name));
  }

  public static void throwInvalidExpression(String name, String expression)
      throws ExpressionException {
    throw new ExpressionException(
        "Found the invalid expression [%s] while evaluating [%s]".formatted(expression, name));
  }

  public static void throwInvalidIndexedExpression(String name, String expression)
      throws ExpressionException {
    throw new ExpressionException(
        "Found the invalid indexed expression [%s] while evaluating [%s]"
            .formatted(expression, name));
  }

  public static void reThrowInvalidExpression(
      ExpressionException cause, String name, String expression) throws ExpressionException {
    throw new ExpressionException(
        "Found the invalid expression [%s] while evaluating [%s]: <%s>"
            .formatted(expression, name, cause.getMessage()));
  }
}

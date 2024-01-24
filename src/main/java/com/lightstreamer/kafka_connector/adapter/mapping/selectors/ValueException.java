package com.lightstreamer.kafka_connector.adapter.mapping.selectors;

public class ValueException extends RuntimeException {

    public ValueException(String message) {
        super(message);
    }

    public static void throwFieldNotFound(String field) throws ValueException {
        throwException(mkException("Field [%s] not found", field));
    }

    public static void throwNoIndexedField() throws ValueException {
        throwException(mkException("Current field is not indexed"));
    }

    public static void throwConversionError(String type) throws ValueException {
        throwException(mkException("Current field is not of type [%s]", type));
    }

    private static void throwException(ValueException ve) throws ValueException {
        throw ve;
    }

    private static ValueException mkException(String message, Object... args) {
        return new ValueException(message.formatted(args));
    }

}
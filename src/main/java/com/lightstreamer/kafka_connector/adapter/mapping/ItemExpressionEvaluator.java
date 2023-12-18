package com.lightstreamer.kafka_connector.adapter.mapping;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lightstreamer.kafka_connector.adapter.mapping.selectors.SelectorExpressionParser;

public interface ItemExpressionEvaluator {

    static class EvaluationException extends Exception {

        EvaluationException(String message) {
            super(message);
        }
    }

    record Result(String prefix, Map<String, String> params) {

        Schema schema() {
            return Schema.of(params.keySet());
        }

    }

    static ItemExpressionEvaluator template() {
        return ItemEvaluator.TEMPLATE;
    }

    static ItemExpressionEvaluator subscribed() {
        return ItemEvaluator.SUBSCRIBED;
    }

    Result eval(String expression) throws EvaluationException;
}

enum ItemEvaluator implements ItemExpressionEvaluator {

    TEMPLATE(Pattern.compile("([a-zA-Z0-9_-]+)(-" + SelectorExpressionParser.SELECTION_REGEX + ")?"),
            Pattern.compile("(([a-zA-Z\\._]\\w*)=([a-zA-Z0-9\\.\\[\\]\\*]+)),?")),

    SUBSCRIBED(Pattern.compile("([a-zA-Z0-9_-]+)(-<(.*)>)?"),
            Pattern.compile("(([a-zA-Z\\._]\\w*)=([^,]+)),?"));

    private final Pattern gobal;

    private final Pattern local;

    private ItemEvaluator(Pattern global, Pattern local) {
        this.gobal = global;
        this.local = local;
    }

    public Result eval(String expression) throws EvaluationException {
        Matcher matcher = gobal.matcher(expression);
        if (!matcher.matches()) {
            throw new RuntimeException("Invalid item");
        }
        Map<String, String> queryParams = new HashMap<>();
        String prefix = matcher.group(1);
        String queryString = matcher.group(3);
        if (queryString != null) {
            Matcher m = local.matcher(queryString);
            int previousEnd = 0;
            while (m.find()) {
                if (m.start() != previousEnd) {
                    break;
                }
                String key = m.group(2);
                String value = m.group(3);
                if (queryParams.put(key, value) != null) {
                    throw new EvaluationException("No duplicated keys are allowed");
                }
                previousEnd = m.end();
            }
            if (previousEnd < queryString.length()) {
                throw new RuntimeException("Invalid query parameter");
            }
        }

        return new Result(prefix, queryParams);
    }
}

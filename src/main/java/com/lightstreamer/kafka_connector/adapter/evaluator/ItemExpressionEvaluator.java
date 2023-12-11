package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lightstreamer.kafka_connector.adapter.consumers.Pair;
import com.lightstreamer.kafka_connector.adapter.evaluator.selectors.SelectorExpressionParser;

public interface ItemExpressionEvaluator {

    static class EvaluationException extends Exception {

        EvaluationException(String message) {
            super(message);
        }
    }

    record Result(String prefix, Set<Pair<String, String>> pairs) {

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

    private final Pattern p1;

    private final Pattern p2;

    private ItemEvaluator(Pattern global, Pattern specific) {
        this.p1 = global;
        this.p2 = specific;
    }

    public Result eval(String expression) throws EvaluationException {
        Matcher matcher = p1.matcher(expression);
        if (!matcher.matches()) {
            throw new RuntimeException("Invalid item");
        }
        Set<Pair<String, String>> queryParams = new LinkedHashSet<>();
        String prefix = matcher.group(1);
        String queryString = matcher.group(3);
        if (queryString != null) {
            Matcher m = p2.matcher(queryString);
            int previousEnd = 0;
            while (m.find()) {
                if (m.start() != previousEnd) {
                    break;
                }
                String key = m.group(2);
                String value = m.group(3);
                if (!queryParams.add(Pair.p(key, value))) {
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

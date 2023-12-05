package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lightstreamer.kafka_connector.adapter.consumers.Pair;

record ExpressionResult(String prefix, List<Pair<String, String>> pairs) {

}

enum ItemExpressionEvaluator {

    TEMPLATE(Pattern.compile("([a-zA-Z0-9_-]+)(-\\#\\{(.*)\\})?"),
            Pattern.compile("(([a-zA-Z\\._]\\w*)=([a-zA-Z0-9\\.\\[\\]\\*]+)),?")),

    SUBSCRIBED(Pattern.compile("([a-zA-Z0-9_-]+)(-<(.*)>)?"), Pattern.compile("(([a-zA-Z\\._]\\w*)=([^,]+)),?"));

    private final Pattern p1;

    private final Pattern p2;

    ItemExpressionEvaluator(Pattern global, Pattern specific) {
        this.p1 = global;
        this.p2 = specific;
    }

    ExpressionResult eval(String expression) {
        Matcher matcher = p1.matcher(expression);
        if (!matcher.matches()) {
            throw new RuntimeException("Invalid item");
        }
        List<Pair<String, String>> queryParams = new ArrayList<>();
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
                queryParams.add(Pair.p(key, value));
                previousEnd = m.end();
            }
            if (previousEnd < queryString.length()) {
                throw new RuntimeException("Invalid query parameter");
            }
        }

        return new ExpressionResult(prefix, queryParams);
    }
}


/*
 * Copyright (C) 2024 Lightstreamer Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package com.lightstreamer.kafka.mapping;

import com.lightstreamer.kafka.mapping.selectors.ExpressionException;
import com.lightstreamer.kafka.mapping.selectors.Parsers;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface ItemExpressionEvaluator {

    record Result(String prefix, Map<String, String> params) {}

    static ItemExpressionEvaluator template() {
        return ItemEvaluator.TEMPLATE;
    }

    static ItemExpressionEvaluator subscribed() {
        return ItemEvaluator.SUBSCRIBED;
    }

    Result eval(String expression) throws ExpressionException;
}

enum ItemEvaluator implements ItemExpressionEvaluator {
    TEMPLATE(
            Pattern.compile("(^[a-zA-Z0-9_-]+)(-" + Parsers.SELECTION_REGEX + ")$"),
            Pattern.compile("(([a-zA-Z\\._]\\w*)=([a-zA-Z0-9\\.\\[\\]\\*]+)),?")) {

        String errorMessage() {
            return "Invalid template expression";
        }
    },

    SUBSCRIBED(
            Pattern.compile("([a-zA-Z0-9_-]+)(-\\[(.*)\\])?"),
            Pattern.compile("(([a-zA-Z\\._]\\w*)=([^,]+)),?"));

    private final Pattern gobal;

    private final Pattern local;

    private ItemEvaluator(Pattern global, Pattern local) {
        this.gobal = global;
        this.local = local;
    }

    String errorMessage() {
        return "Invalid Item";
    }

    /**
     * @throws ExpressionException
     */
    public Result eval(String expression) throws ExpressionException {
        Matcher matcher = gobal.matcher(expression);
        if (!matcher.matches()) {
            throw new ExpressionException(errorMessage());
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
                    throw new ExpressionException("No duplicated keys are allowed");
                }
                previousEnd = m.end();
            }
            if (previousEnd < queryString.length()) {
                throw new ExpressionException(errorMessage());
            }
        }

        return new Result(prefix, queryParams);
    }
}

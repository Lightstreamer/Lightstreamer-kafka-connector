package com.lightstreamer.kafka_connector.adapter.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lightstreamer.kafka_connector.adapter.consumers.JsonNodeEvaluator;

public class StatementEvaluator<T> {

    static record Replacement<T>(String placeHolder, RecordEvaluator<T> evaluator) {

        public String replace(String source, T record) {
            String value = evaluator.extract(record);
            return source.replace(placeHolder, value);
        }
    }

    private final String statement;

    private final List<Replacement<T>> replacements = new ArrayList<>();

    private static final Pattern pattern = Pattern.compile("\\$\\{([\\w\\.]+)\\}");

    private final EvaluatorFactory<T> ref;

    private String replaced;

    public StatementEvaluator(String statement, EvaluatorFactory<T> ref) {
        System.out.println("Creating navigator for " + statement);
        Objects.requireNonNull(statement);
        this.statement = statement;
        this.ref = ref;
        this.replaced = analyze(statement,
                matcher -> replacements.add(new Replacement<>(matcher.group(0), ref.get(matcher.group(1)))));
    }

    private String analyze(String spec, Consumer<Matcher> hook) {
        Matcher matcher = pattern.matcher(spec);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(sb, "TOK");
            hook.accept(matcher);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public String replace(T record) {
        String source = statement;
        for (Replacement<T> re : replacements) {
            source = re.replace(source, record);
        }
        return source;
    }

    public boolean match(String input) {
        return replaced.equals(analyze(input, m -> {
        }));
    }

    static class User {
        // @JsonProperty
        public String firstName;

        // @JsonProperty
        public String lastName;

        // @JsonProperty
        public short age;

        public User(String firstName, String lastName, short age) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }

    }

    public static void main(String[] args) {
        var s = new StatementEvaluator<JsonNode>("kafka-user-${firstName}-${age}-suffix", JsonNodeEvaluator::new);
        ObjectMapper om = new ObjectMapper();
        JsonNode node = om.valueToTree(new User("Gianluca", "Finocchiaro", (short) 48));
        System.out.println(node);
        var replaced = s.replace(node);
        System.out.println(replaced);
        String t = "kafka-user-${gianluca}-${48}-suffix";
        System.out.println(s.match(t));

    }
}

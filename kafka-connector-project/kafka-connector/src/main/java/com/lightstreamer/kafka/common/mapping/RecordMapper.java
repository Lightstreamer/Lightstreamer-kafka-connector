
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

package com.lightstreamer.kafka.common.mapping;

import static java.util.Collections.emptySet;

import com.lightstreamer.kafka.common.mapping.Items.SubscribedItem;
import com.lightstreamer.kafka.common.mapping.Items.SubscribedItems;
import com.lightstreamer.kafka.common.mapping.RecordMapper.MappedRecord;
import com.lightstreamer.kafka.common.mapping.selectors.DataExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.FieldsExtractor;
import com.lightstreamer.kafka.common.mapping.selectors.KafkaRecord;
import com.lightstreamer.kafka.common.mapping.selectors.ValueException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The {@code RecordMapper} interface provides a mechanism for transforming Kafka records into
 * Lightstreamer items through template-based extraction and data mapping. It defines methods for
 * extracting canonical item names, managing data extractors for topic subscriptions, and bridging
 * Kafka's record-based architecture with Lightstreamer's item-based publish-subscribe model.
 *
 * <p>The mapping process transforms Kafka records into structured Lightstreamer updates by:
 *
 * <ul>
 *   <li><strong>Template expansion:</strong> Evaluating configured templates against record data to
 *       generate canonical item names for subscriber routing
 *   <li><strong>Field extraction:</strong> Processing record payloads to extract structured field
 *       data for client updates
 *   <li><strong>Topic-based routing:</strong> Managing extractors per topic to ensure proper data
 *       transformation based on the record's origin
 * </ul>
 *
 * @param <K> the type of the key in the Kafka record
 * @param <V> the type of the value in the Kafka record
 */
public interface RecordMapper<K, V> {

    /**
     * Represents the result of mapping a Kafka record through template-based extraction. A {@code
     * MappedRecord} contains canonical item names derived from templates and provides lazy access
     * to field data for routing to Lightstreamer subscribers.
     *
     * <p>The mapping process transforms Kafka records into Lightstreamer items by:
     *
     * <ul>
     *   <li>Expanding templates into concrete item names using record data
     *   <li>Providing routing capabilities to match against subscriber subscriptions
     *   <li>Offering lazy field extraction for data updates when needed
     * </ul>
     *
     * <p><strong>Canonical form guarantee:</strong> All item names are returned in canonical form
     * with fields arranged in consistent alphabetical order by the statically configured template
     * field names, ensuring deterministic output regardless of the original template definition
     * order.
     *
     * <p><strong>Configuration-independent field ordering example:</strong> If a template is
     * configured as: {@code
     * "item-#{userId=KEY.userId,accountId=VALUE.accountId,profileId=VALUE.profileId}"} The
     * resulting item name will always arrange fields alphabetically: {@code
     * "item-[accountId=123,profileId=456,userId=789]"} - with fields ordered by their configured
     * names (accountId, profileId, userId), not by their definition order in the template.
     */
    interface MappedRecord {

        /**
         * Returns the canonical Lightstreamer item names that this Kafka record maps to after
         * template processing. Each name represents a distinct Lightstreamer item that should
         * receive data updates when this record is processed.
         *
         * <p>These item names are produced by evaluating item templates against the Kafka record's
         * data. Templates containing placeholders like {@code {VALUE.symbol}} or {@code {KEY.id}}
         * are resolved into concrete item names using the actual field values from the record.
         *
         * <p><strong>Examples:</strong>
         *
         * <ul>
         *   <li>Template {@code "stock-#{symbol=VALUE.symbol}"} → Item name {@code
         *       "stock-[symbol=AAPL]"}
         *   <li>Template {@code "user-profile-#{userId=KEY.userId}"} → Item name {@code
         *       "user-profile-[userId=12345]"}
         *   <li>Static template {@code "market-summary"} → Item name {@code "market-summary"}
         * </ul>
         *
         * <p>The returned item names are used primarily for:
         *
         * <ul>
         *   <li>Routing records to subscribers of matching items
         *   <li>Identifying which Lightstreamer items need data updates
         *   <li>Subscription management and filtering
         * </ul>
         *
         * <p><strong>Canonical form guarantee:</strong> Each returned item name is in canonical
         * form with fields arranged alphabetically by the statically configured template field
         * names.
         *
         * <p><strong>Configuration-independent field ordering example:</strong> If a template is
         * configured as: {@code
         * "item-#{userId=KEY.userId,accountId=VALUE.accountId,profileId=VALUE.profileId}"} The
         * resulting item name will always arrange fields alphabetically: {@code
         * "item-[accountId=123,profileId=456,userId=789]"} - with fields ordered by their
         * configured names (accountId, profileId, userId), not by their definition order in the
         * template.
         *
         * @return an array of canonical Lightstreamer item names derived from template evaluation;
         *     never null but may be empty
         */
        String[] canonicalItemNames();

        /**
         * Provides lazy access to the field data extracted from the Kafka record for data updates.
         * This map contains the structured field values that will be sent to Lightstreamer
         * subscribers when items matching this record receive updates.
         *
         * <p>Unlike {@link #canonicalItemNames()} which handles routing through template expansion,
         * this method extracts the actual data payload using field extractors. The field names in
         * the returned map correspond to the Lightstreamer schema fields that subscribers will
         * receive.
         *
         * <p><strong>Key characteristics:</strong>
         *
         * <ul>
         *   <li><strong>Lazy evaluation:</strong> Field extraction is performed on-demand to
         *       optimize performance when only routing information is needed
         *   <li><strong>Schema-based:</strong> Field names match the configured schema for
         *       consistent data structure
         *   <li><strong>Update payload:</strong> Contains the actual values that will be pushed to
         *       subscribers
         * </ul>
         *
         * <p><strong>Example:</strong> For a stock record, this might return: {@code {"symbol":
         * "AAPL", "price": "150.25", "timestamp": "2024-01-15T10:30:00Z"}}
         *
         * @return a map of field names to their extracted values from the record; never null but
         *     may be empty
         * @throws ValueException if field extraction fails due to data format issues or missing
         *     required fields
         */
        Map<String, String> fieldsMap() throws ValueException;

        /**
         * Determines which subscribed items should receive this record by matching the item names
         * from template expansion against active client subscriptions. This method performs the
         * critical routing function that connects Kafka data to specific Lightstreamer subscribers.
         *
         * <p>The routing process works by:
         *
         * <ul>
         *   <li>Taking the item names from {@link #canonicalItemNames()} (e.g.,
         *       "stock-[symbol=AAPL]")
         *   <li>Looking up each name in the provided subscribed items collection
         *   <li>Returning only those items that have active subscribers
         * </ul>
         *
         * <p><strong>Filtering behavior:</strong> Only items with active subscriptions are
         * returned, which means records that don't match any current subscriptions will be filtered
         * out. This ensures that data updates are only sent to clients who have actually subscribed
         * to the corresponding items.
         *
         * <p><strong>Example:</strong> If this record maps to item names ["stock-[symbol=AAPL]",
         * "stock-[symbol=MSFT]"] but only "stock-[symbol=AAPL]" has active subscribers, only that
         * item will be returned.
         *
         * @param subscribed the collection of currently active item subscriptions to match against
         * @return the subset of subscribed items that match this record's item names; never null
         *     but may be empty
         */
        Set<SubscribedItem> route(SubscribedItems subscribed);
    }

    /**
     * Retrieves the set of template extractors configured for the specified Kafka topic. Template
     * extractors are responsible for generating canonical item names through template expansion
     * when records from the given topic are processed by {@link #map(KafkaRecord)}.
     *
     * <p>This method enables inspection of the mapping configuration and is primarily used for
     * diagnostics, testing, and validation of extractor configurations.
     *
     * @param topicName the name of the Kafka topic to retrieve extractors for
     * @return a set of data extractors configured for the specified topic; never null but may be
     *     empty if no extractors are configured for the topic
     */
    Set<DataExtractor<K, V>> getExtractorsByTopicSubscription(String topicName);

    /**
     * Transforms a Kafka record into a {@code MappedRecord} containing canonical item names and
     * lazy field extraction capabilities for routing to Lightstreamer subscribers.
     *
     * <p>This method is the core transformation function that bridges Kafka data with
     * Lightstreamer's item-based architecture. It performs template expansion to determine which
     * Lightstreamer items this record should update and prepares the field data for eventual
     * extraction.
     *
     * <p><strong>Processing steps:</strong>
     *
     * <ol>
     *   <li>Identifies template extractors for the record's topic
     *   <li>Evaluates each template against the record data to produce canonical item names
     *   <li>Sets up lazy field extraction using the configured field extractor
     *   <li>Returns a {@code MappedRecord} containing both routing and data information
     * </ol>
     *
     * <p><strong>Example:</strong> A record from topic "stocks" with value containing {@code
     * {"symbol": "AAPL", "price": 150.25}} processed with template {@code
     * "stock-#{symbol=VALUE.symbol}"} would produce a {@code MappedRecord} with canonical item name
     * {@code "stock-[symbol=AAPL]"} and field data available on demand.
     *
     * <p><strong>No-op behavior:</strong> If no template extractors are configured for the record's
     * topic, returns a no-operation record that produces no routing targets and empty field data.
     *
     * @param record the Kafka record to be mapped into Lightstreamer format
     * @return a {@code MappedRecord} containing canonical item names and lazy field access; never
     *     null but may represent a no-operation mapping
     * @throws ValueException if template expansion fails due to data extraction issues
     */
    MappedRecord map(KafkaRecord<K, V> record) throws ValueException;

    /**
     * Indicates whether this mapper has any template extractors configured for generating canonical
     * item names. Template extractors are used by {@link #map(KafkaRecord)} to determine routing
     * targets through template expansion.
     *
     * @return {@code true} if at least one template extractor is configured, {@code false}
     *     otherwise
     */
    boolean hasExtractors();

    /**
     * Indicates whether this mapper has a field extractor configured for generating data updates.
     * Field extractors are used by {@link MappedRecord#fieldsMap()} to provide structured field
     * data for Lightstreamer client updates.
     *
     * @return {@code true} if a field extractor is configured, {@code false} if using the default
     *     no-operation extractor
     */
    boolean hasFieldExtractor();

    /**
     * Indicates whether regex pattern matching is enabled for topic-to-extractor associations. When
     * enabled, topic names in extractor configurations are treated as regular expressions rather
     * than literal topic names.
     *
     * @return {@code true} if regex matching is enabled, {@code false} for exact topic name
     *     matching
     */
    boolean isRegexEnabled();

    static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * A no-operation data extractor that produces empty results for all extraction operations. This
     * extractor serves as the default field extractor when no specific field extraction
     * configuration is provided, ensuring that {@link MappedRecord#fieldsMap()} operations complete
     * successfully without actual data extraction.
     */
    static class NOPDataExtractor<K, V> implements FieldsExtractor<K, V> {

        @Override
        public void extractIntoMap(KafkaRecord<K, V> record, Map<String, String> targetMap)
                throws ValueException {}

        @Override
        public boolean skipOnFailure() {
            return false;
        }

        @Override
        public boolean mapNonScalars() {
            return false;
        }

        @Override
        public Set<String> mappedFields() {
            return Collections.emptySet();
        }
    }

    /**
     * Builder for constructing {@code RecordMapper} instances with template extractors, field
     * extractors, and configuration options for Kafka-to-Lightstreamer record transformation.
     */
    static class Builder<K, V> {

        static final FieldsExtractor<?, ?> NOP = new NOPDataExtractor<>();

        final Map<String, Set<DataExtractor<K, V>>> extractorsByTopicSubscription = new HashMap<>();

        @SuppressWarnings("unchecked")
        FieldsExtractor<K, V> fieldExtractor = (FieldsExtractor<K, V>) NOP;

        boolean regexEnabled = false;

        private Builder() {}

        /**
         * Adds multiple template extractors for canonical item name generation. Template extractors
         * are used by {@link RecordMapper#map(KafkaRecord)} to produce routing targets through
         * template expansion.
         *
         * @param templateExtractors a map of topic names (or patterns) to sets of template
         *     extractors
         * @return this builder for method chaining
         */
        public Builder<K, V> withTemplateExtractors(
                Map<String, Set<DataExtractor<K, V>>> templateExtractors) {
            this.extractorsByTopicSubscription.putAll(templateExtractors);
            return this;
        }

        /**
         * Adds a single template extractor for the specified topic subscription. Multiple
         * extractors can be associated with the same topic to generate multiple canonical item
         * names from a single record.
         *
         * @param subscription the topic name or pattern (if regex is enabled) to associate the
         *     extractor with
         * @param templateExtractor the template extractor for generating canonical item names
         * @return this builder for method chaining
         */
        public final Builder<K, V> withTemplateExtractor(
                String subscription, DataExtractor<K, V> templateExtractor) {
            this.extractorsByTopicSubscription.compute(
                    subscription,
                    (t, extractors) -> {
                        if (extractors == null) {
                            extractors = new HashSet<>();
                        }
                        extractors.add(templateExtractor);
                        return extractors;
                    });
            return this;
        }

        /**
         * Enables or disables regex pattern matching for topic-to-extractor associations. When
         * enabled, topic names in extractor configurations are treated as regular expressions.
         *
         * @param enable {@code true} to enable regex matching, {@code false} for exact topic name
         *     matching
         * @return this builder for method chaining
         */
        public final Builder<K, V> enableRegex(boolean enable) {
            this.regexEnabled = enable;
            return this;
        }

        /**
         * Sets the field extractor for generating structured data updates. The field extractor is
         * used by {@link MappedRecord#fieldsMap()} to provide field data for Lightstreamer client
         * updates.
         *
         * @param extractor the field extractor for data extraction; if not set, a no-operation
         *     extractor will be used
         * @return this builder for method chaining
         */
        public final Builder<K, V> withFieldExtractor(FieldsExtractor<K, V> extractor) {
            this.fieldExtractor = extractor;
            return this;
        }

        /**
         * Constructs the {@code RecordMapper} instance with the configured template extractors,
         * field extractor, and options.
         *
         * @return a new RecordMapper instance ready for Kafka record transformation
         */
        public RecordMapper<K, V> build() {
            return new DefaultRecordMapper<>(this);
        }
    }
}

final class DefaultRecordMapper<K, V> implements RecordMapper<K, V> {

    protected static Logger log = LoggerFactory.getLogger(DefaultRecordMapper.class);

    interface ExtractorsSupplier<K, V> {

        Collection<DataExtractor<K, V>> getExtractors(String topic);
    }

    static record PatternAndExtractors<K, V>(
            Pattern pattern, Set<DataExtractor<K, V>> extractors) {}

    private final FieldsExtractor<K, V> fieldExtractor;
    private final Map<String, Set<DataExtractor<K, V>>> templateExtractors;
    private final Collection<PatternAndExtractors<K, V>> patterns;
    private final ExtractorsSupplier<K, V> extractorsSupplier;
    private final boolean regexEnabled;

    DefaultRecordMapper(Builder<K, V> builder) {
        this.fieldExtractor = builder.fieldExtractor;
        this.templateExtractors =
                Collections.unmodifiableMap(builder.extractorsByTopicSubscription);
        this.regexEnabled = builder.regexEnabled;
        this.patterns = mayFillPatternsList();
        this.extractorsSupplier =
                regexEnabled ? this::getMatchingExtractors : this::getAssociatedExtractors;
    }

    private Collection<PatternAndExtractors<K, V>> mayFillPatternsList() {
        if (!regexEnabled) {
            return Collections.emptyList();
        }

        Collection<PatternAndExtractors<K, V>> pe = new ArrayList<>();
        Set<String> topics = templateExtractors.keySet();
        for (String topicRegEx : topics) {
            pe.add(
                    new PatternAndExtractors<>(
                            Pattern.compile(topicRegEx), templateExtractors.get(topicRegEx)));
        }
        return pe;
    }

    private Collection<DataExtractor<K, V>> getAssociatedExtractors(String topic) {
        return templateExtractors.getOrDefault(topic, emptySet());
    }

    private Collection<DataExtractor<K, V>> getMatchingExtractors(String topic) {
        Collection<DataExtractor<K, V>> extractors = new ArrayList<>();
        for (PatternAndExtractors<K, V> p : patterns) {
            Matcher matcher = p.pattern().matcher(topic);
            if (matcher.matches()) {
                extractors.addAll(p.extractors());
            }
        }
        return extractors;
    }

    @Override
    public Set<DataExtractor<K, V>> getExtractorsByTopicSubscription(String topicName) {
        return templateExtractors.get(topicName);
    }

    @Override
    public boolean hasExtractors() {
        return !templateExtractors.isEmpty();
    }

    @Override
    public boolean hasFieldExtractor() {
        return fieldExtractor != Builder.NOP;
    }

    @Override
    public boolean isRegexEnabled() {
        return this.regexEnabled;
    }

    @Override
    public MappedRecord map(KafkaRecord<K, V> record) throws ValueException {
        var extractors = extractorsSupplier.getExtractors(record.topic());

        if (extractors.isEmpty()) {
            return DefaultMappedRecord.NOPRecord;
        }

        String[] canonicalItems = new String[extractors.size()];
        int i = 0;
        for (DataExtractor<K, V> dataExtractor : extractors) {
            canonicalItems[i++] = dataExtractor.extractAsCanonicalItem(record);
        }

        return new DefaultMappedRecord(canonicalItems, () -> fieldExtractor.extractMap(record));
    }
}

final class DefaultMappedRecord implements MappedRecord {

    private static final Supplier<Map<String, String>> EMPTY_FIELDS_MAP = Collections::emptyMap;

    static final MappedRecord NOPRecord = new DefaultMappedRecord();

    private final String[] itemNames;

    // Lazy supplier for fieldsMap. It is used to avoid computing the fieldsMap
    // when not needed (i.e. when routing only).
    private final Supplier<Map<String, String>> fieldsMapSupplier;

    private DefaultMappedRecord() {
        this(new String[0], EMPTY_FIELDS_MAP);
    }

    DefaultMappedRecord(String[] itemNames) {
        this(itemNames, EMPTY_FIELDS_MAP);
    }

    DefaultMappedRecord(String[] canonicalItems, Supplier<Map<String, String>> fieldsMap) {
        this.itemNames = canonicalItems;
        this.fieldsMapSupplier = fieldsMap;
    }

    @Override
    public String[] canonicalItemNames() {
        return itemNames;
    }

    @Override
    public Set<SubscribedItem> route(SubscribedItems subscribedItems) {
        Set<SubscribedItem> result = new HashSet<>();

        for (String itemName : itemNames) {
            SubscribedItem item = subscribedItems.get(itemName);
            if (item != null) {
                result.add(item);
            }
        }
        return result;
    }

    @Override
    public Map<String, String> fieldsMap() {
        return fieldsMapSupplier.get();
    }

    @Override
    public String toString() {
        String data = Arrays.stream(itemNames).collect(Collectors.joining(","));
        return String.format(
                "MappedRecord (canonicalItemNames=[%s], fieldsMap=%s)",
                data, fieldsMapSupplier.get());
    }
}

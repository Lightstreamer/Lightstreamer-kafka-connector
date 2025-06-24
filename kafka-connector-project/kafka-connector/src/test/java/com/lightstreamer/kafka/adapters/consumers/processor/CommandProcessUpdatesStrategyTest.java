
/*
 * Copyright (C) 2025 Lightstreamer Srl
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

package com.lightstreamer.kafka.adapters.consumers.processor;

import static com.google.common.truth.Truth.assertThat;

import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.CommandMode.Command;
import com.lightstreamer.kafka.adapters.consumers.processor.RecordConsumerSupport.CommandProcessUpdatesStrategy;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class CommandProcessUpdatesStrategyTest {

    @ParameterizedTest(name = "[{index}] {arguments}")
    @CsvSource(
            useHeadersInDisplayName = true,
            delimiter = '|',
            textBlock =
                    """
                    KEY        | COMMAND     | EXPECTED
                    aKey       | ADD         | true
                    aKey       | UPDATE      | true
                    aKey       | DELETE      | true
                    aKey       | UNKNOWN     | false
                    aKey       | <NOCOMMAND> | false
                    aKey       | <EMPTY>     | false
                    aKey       | CS          | false
                    aKey       | EOS         | false
                    aKey       | <NULL>      | false
                    <NOKEY>    | ADD         | false
                    <NULL>     | ADD         | false
                    <NULL>     | UPDATE      | false
                    <NULL>     | DELETE      | false
                    <EMPTY>    | DELETE      | false
                    snapshot   | CS          | true
                    snapshot   | EOS         | true
                    snapshot   | ADD         | false
                    snapshot   | DELETE      | false
                    snapshot   | UPDATE      | false
                    snapshot   | UNKNOWN     | false
                    snapshot   | <NOCOMMAND> | false
                    """)
    void shouldCheckValidInput(String key, String command, boolean expected) {
        CommandProcessUpdatesStrategy commandStrategy = new CommandProcessUpdatesStrategy();
        Map<String, String> input = new HashMap<>();
        switch (key) {
            case "<NOKEY>" -> {}
            case "<EMPTY>" -> input.put("key", "");
            case "<NULL>" -> input.put("key", null);
            default -> input.put("key", key);
        }

        switch (command) {
            case "<NOCOMMAND>" -> {}
            case "<EMPTY>" -> input.put("command", "");
            case "<NULL>" -> input.put("command", null);
            default -> input.put("command", command);
        }

        Optional<Command> checkInput = commandStrategy.checkInput(input);
        if (expected) {
            assertThat(checkInput).isPresent();
            assertThat(checkInput.get()).isEqualTo(Command.valueOf(command));
        } else {
            assertThat(checkInput).isEmpty();
        }
    }
}

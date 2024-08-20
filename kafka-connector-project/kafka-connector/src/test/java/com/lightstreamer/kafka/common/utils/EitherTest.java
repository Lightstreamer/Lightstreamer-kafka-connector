
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

package com.lightstreamer.kafka.common.utils;

import static com.google.common.truth.Truth.assertThat;

import static org.junit.Assert.assertThrows;

import org.junit.jupiter.api.Test;

public class EitherTest {

    @Test
    void shouldBeLeft() {
        Either<String, ?> left = Either.left("left");
        assertThat(left.isLeft()).isTrue();
        assertThat(left.isRight()).isFalse();
        assertThat(left.getLeft()).isEqualTo("left");
        assertThrows(RuntimeException.class, () -> left.getRight());
    }

    @Test
    void shouldBeRight() {
        Either<?, Integer> right = Either.right(1);
        assertThat(right.isLeft()).isFalse();
        assertThat(right.isRight()).isTrue();
        assertThrows(RuntimeException.class, () -> right.getLeft());
        assertThat(right.getRight()).isEqualTo(1);
    }

    @Test
    void shouldSupportNull() {
        // Either nullassertThrows(IllegalArgumentException.class, () -> Either.left(null));
        // assertThrows(IllegalArgumentException.class, () -> Either.right(null));
    }

    @Test
    void shouldHaveSameHashCode() {
        Either<String, ?> left1 = Either.left("left");
        Either<String, ?> left2 = Either.left("left");
        assertThat(left1.hashCode()).isEqualTo(left2.hashCode());
    }

    @Test
    void shouldNotHaveSameHashCode() {
        Either<String, ?> left1 = Either.left("left1");
        Either<String, ?> left2 = Either.left("left2");
        assertThat(left1.hashCode()).isNotEqualTo(left2.hashCode());
    }

    @Test
    void shouldBeEqual() {
        Either<String, ?> left1 = Either.left("left");
        Either<String, ?> left2 = Either.left("left");
        assertThat(left1.equals(left1)).isTrue();
        assertThat(left1.equals(left2)).isTrue();

        Either<String, ?> right1 = Either.right("right");
        Either<String, ?> right2 = Either.right("right");
        assertThat(right1.equals(right1)).isTrue();
        assertThat(right1.equals(right2)).isTrue();
    }

    @Test
    void shouldNotBeEqual() {
        Either<String, ?> left1 = Either.left("left1");
        Either<String, ?> left2 = Either.left("left2");
        assertThat(left1.equals(left2)).isFalse();

        Either<String, ?> right1 = Either.right("right1");
        Either<String, ?> right2 = Either.right("right2");
        assertThat(right1.equals(right2)).isFalse();

        assertThat(left1.equals(right2)).isFalse();
    }
}

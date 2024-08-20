
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

import java.util.Objects;

public abstract class Either<L, R> {

    private static class Left<L, R> extends Either<L, R> {

        private Left(L left) {
            super(left);
        }

        @Override
        public boolean isLeft() {
            return true;
        }

        @Override
        public boolean isRight() {
            return false;
        }

        @SuppressWarnings("unchecked")
        @Override
        public L getLeft() {
            return (L) value;
        }

        @Override
        public R getRight() {
            throw new RuntimeException("Can't access an undefined object");
        }
    }

    private static class Right<L, R> extends Either<L, R> {

        private Right(R right) {
            super(right);
        }

        @Override
        public boolean isLeft() {
            return false;
        }

        @Override
        public boolean isRight() {
            return true;
        }

        @Override
        public L getLeft() {
            throw new RuntimeException("Can't access an undefined object");
        }

        @SuppressWarnings("unchecked")
        @Override
        public R getRight() {
            return (R) value;
        }
    }

    protected final Object value;

    Either(Object value) {
        this.value = value;
    }

    public abstract L getLeft();

    public abstract R getRight();

    public abstract boolean isLeft();

    public abstract boolean isRight();

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;

        return obj instanceof Either other && Objects.equals(value, other.value);
    }

    public static <L, R> Either<L, R> left(L left) {
        return new Left<>(left);
    }

    public static <L, R> Either<L, R> right(R right) {
        return new Right<>(right);
    }
}


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

package com.lightstreamer.kafka_connector.adapter.commons;

public class Either<Left, Right> {

    private final Left left;

    private final Right right;

    private Either(Left left, Right right) {
        if (left != null & right == null) {
            this.left = left;
            this.right = null;
            return;
        }
        if (left == null && right != null) {
            this.left = null;
            this.right = right;
            return;
        }
        throw new IllegalArgumentException("Only one parameter can be specified");
    }

    public Left getLeft() {
        return left;
    }

    public Right getRight() {
        return right;
    }

    public boolean isLeft() {
        return left != null;
    }

    public boolean isRight() {
        return right != null;
    }

    public static <Left, Right> Either<Left, Right> left(Left left) {
        return new Either<>(left, null);
    }

    public static <Left, Right> Either<Left, Right> right(Right right) {
        return new Either<>(null, right);
    }
}

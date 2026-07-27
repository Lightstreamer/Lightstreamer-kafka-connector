
/*
 * Copyright (C) 2026 Lightstreamer Srl
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

package com.lightstreamer.kafka.common.annotations;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a type, method, constructor, or field is exposed at a wider visibility than its
 * production usage requires, solely to enable access from tests.
 *
 * <p>Callers other than tests should treat annotated members as {@code private} (or as an
 * implementation detail if applied to a type) and must not depend on them.
 *
 * <p>Annotation retention is {@link RetentionPolicy#SOURCE} — the marker is dropped at compile
 * time, imposes no runtime cost, and adds no dependency to the compiled classpath. Its purpose is
 * documentation and static-analysis attribution.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({TYPE, METHOD, CONSTRUCTOR, FIELD})
public @interface VisibleForTesting {}

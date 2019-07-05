/*
 * Copyright 2019 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.api.v0;

import java.util.List;
import org.msgpack.value.ImmutableValue;

/**
 * In-process (in-JVM) container of data records.
 *
 * <p>It serializes records into {@code byte[]} (in {@link org.embulk.api.v0.Buffer}) in order to:
 *
 * <ul>
 * <li> A) Avoid slowness by handling many Java Objects
 * <li> B) Avoid complexity by type-safe primitive arrays
 * <li> C) Track memory consumption by records
 * <li> D) Use off-heap memory
 * </ul>
 *
 * <p>(C) and (D) may not be so meaningful as of v0.7+ (or since earlier) as recent Embulk unlikely
 * allocates so many Pages at the same time. Recent Embulk is streaming-driven instead of
 * multithreaded queue-based.
 *
 * <p>{@link Page} is NOT for inter-process communication. For multi-process execution such as MapReduce
 * Executor, the executor plugin takes responsibility about interoperable serialization.
 */
public interface Page {
    Page setStringReferences(List<String> values);

    Page setValueReferences(List<ImmutableValue> values);

    List<String> getStringReferences();

    List<ImmutableValue> getValueReferences();

    String getStringReference(int index);

    ImmutableValue getValueReference(int index);

    void release();

    Buffer buffer();
}

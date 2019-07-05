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

package org.embulk.spi.v0;

import java.util.Optional;
import org.embulk.api.v0.Buffer;

public interface FileInput extends AutoCloseable {
    boolean nextFile();

    Buffer poll();

    void close();

    // Gets a text that hints the name of the current file input.
    //
    // <p>The hint is aimed for logging, not for any data recorded in rows / columns.
    // There is no any guarantee on the text. The text format may change in future versions.
    // The text may be lost by putting another plugin in the configuration.
    //
    // @return the hint text
    default Optional<String> hintOfCurrentInputFileNameForLogging() {
        return Optional.empty();
    }
}

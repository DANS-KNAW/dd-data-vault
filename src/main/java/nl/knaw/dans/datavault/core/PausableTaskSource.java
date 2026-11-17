/*
 * Copyright (C) 2024 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.datavault.core;

import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.util.pollingtaskexec.TaskSource;

import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/**
 * A {@link TaskSource} that hands out no tasks while paused, for example during a moratorium on network actions.
 *
 * @param <R> the type of the task records
 */
@RequiredArgsConstructor
public class PausableTaskSource<R> implements TaskSource<R> {
    private final TaskSource<R> delegate;
    private final BooleanSupplier paused;

    @Override
    public Optional<R> nextInput() {
        return paused.getAsBoolean() ? Optional.empty() : delegate.nextInput();
    }

    @Override
    public List<R> nextInputs() {
        return paused.getAsBoolean() ? List.of() : delegate.nextInputs();
    }
}

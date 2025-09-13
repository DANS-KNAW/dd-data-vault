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

import io.dropwizard.hibernate.UnitOfWork;
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.layerstore.ItemsMismatchException;
import nl.knaw.dans.layerstore.Layer;
import nl.knaw.dans.layerstore.LayerConsistencyChecker;

import java.io.IOException;

@RequiredArgsConstructor
public class UnitOfWorkDeclaringLayerConsistencyChecker implements LayerConsistencyChecker {
    private final LayerConsistencyChecker delegate;

    @UnitOfWork
    @Override
    public void check(Layer layer) throws IOException, ItemsMismatchException {
        delegate.check(layer);
    }
}

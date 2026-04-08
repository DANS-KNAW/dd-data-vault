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
import nl.knaw.dans.layerstore.Item;
import nl.knaw.dans.layerstore.ItemStore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

@RequiredArgsConstructor
public class UnitOfWorkDeclaringItemStore implements ItemStore {
    private final ItemStore delegate;

    @Override
    @UnitOfWork
    public List<Item> listDirectory(String directoryPath) throws IOException {
        return delegate.listDirectory(directoryPath);
    }

    @Override
    @UnitOfWork
    public List<Item> listRecursive(String directoryPath) throws IOException {
        return delegate.listRecursive(directoryPath);
    }

    @Override
    @UnitOfWork
    public boolean existsPathLike(String path) {
        return delegate.existsPathLike(path);
    }

    @Override
    @UnitOfWork
    public InputStream readFile(String path) throws IOException {
        return delegate.readFile(path);
    }

    @UnitOfWork
    @Override
    public void writeFile(String path, InputStream content) throws IOException {
        delegate.writeFile(path, content);
    }

    @UnitOfWork
    @Override
    public void moveDirectoryInto(Path source, String destination) throws IOException {
        delegate.moveDirectoryInto(source, destination);
    }

    @UnitOfWork
    @Override
    public void moveDirectoryInternal(String source, String destination) throws IOException {
        delegate.moveDirectoryInternal(source, destination);
    }

    @UnitOfWork
    @Override
    public void deleteDirectory(String path) throws IOException {
        delegate.deleteDirectory(path);
    }

    @UnitOfWork
    @Override
    public void deleteFiles(List<String> paths) throws IOException {
        delegate.deleteFiles(paths);
    }

    @UnitOfWork
    @Override
    public void createDirectory(String path) throws IOException {
        delegate.createDirectory(path);
    }

    @Override
    @UnitOfWork
    public void copyDirectoryOutOf(String source, Path destination) throws IOException {
        delegate.copyDirectoryOutOf(source, destination);
    }
}

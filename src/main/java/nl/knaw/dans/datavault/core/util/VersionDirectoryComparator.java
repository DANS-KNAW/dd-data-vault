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
package nl.knaw.dans.datavault.core.util;

import java.nio.file.Path;
import java.util.Comparator;

public class VersionDirectoryComparator implements Comparator<Path> {
    public static final VersionDirectoryComparator INSTANCE = new VersionDirectoryComparator();
    
    @Override
    public int compare(Path p1, Path p2) {
        if (!p1.getFileName().toString().startsWith("v") || !p2.getFileName().toString().startsWith("v")) {
            throw new IllegalArgumentException("Version directory names should start with 'v'");
        }
        try {
            Long l1 = Long.parseLong(p1.getFileName().toString().substring(1));
            Long l2 = Long.parseLong(p2.getFileName().toString().substring(1));
            return l1.compareTo(l2);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Version directory names should start with 'v' followed by a number");
        }
    }
}

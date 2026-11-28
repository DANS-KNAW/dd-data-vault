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
package nl.knaw.dans.datavault.resources;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Parses version numbers as passed in API paths.
 */
final class VersionNumbers {
    static final String LATEST_FLAG = "latest";
    private static final Pattern V_PREFIXED_NUMBER_PATTERN = Pattern.compile("v\\d+");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

    private VersionNumbers() {
    }

    /**
     * Converts a version number with or without 'v' prefix (e.g. '1' or 'v1') to OCFL format (e.g. 'v1').
     *
     * @param nr the version number
     * @return the version number in OCFL format, or empty if the version number is not valid; 'latest' is not considered valid
     */
    static Optional<String> toOcflFormat(String nr) {
        if (nr == null) {
            return Optional.empty();
        }
        if (V_PREFIXED_NUMBER_PATTERN.matcher(nr).matches()) {
            return Optional.of(nr);
        }
        if (NUMBER_PATTERN.matcher(nr).matches()) {
            return Optional.of("v" + nr);
        }
        return Optional.empty();
    }

    static boolean isLatest(String nr) {
        return LATEST_FLAG.equalsIgnoreCase(nr);
    }
}

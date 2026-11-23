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
package nl.knaw.dans.datavault.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.dropwizard.util.Duration;
import lombok.Data;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@Data
public class MoratoriumConfig {
    @NotNull
    private List<@NotNull String> triggerOn = List.of("Connection refused", "Connection timed out");

    @NotNull
    private Duration duration = Duration.minutes(15);

    @JsonIgnore
    @AssertTrue(message = "triggerOn must contain valid regular expressions")
    public boolean isTriggerOnValidRegexes() {
        return triggerOn == null || triggerOn.stream()
            .filter(Objects::nonNull)
            .allMatch(MoratoriumConfig::isValidRegex);
    }

    private static boolean isValidRegex(String regex) {
        try {
            Pattern.compile(regex);
            return true;
        }
        catch (PatternSyntaxException e) {
            return false;
        }
    }
}

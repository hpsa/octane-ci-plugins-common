/*
 *     Copyright 2017 EntIT Software LLC, a Micro Focus company, L.P.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.hp.octane.integrations.services.entities;


import com.hp.octane.integrations.utils.SdkStringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Created by berkovir on 17/05/2017.
 */
public class QueryHelper {

    public static String conditionRef(String name, long id) {
        return name + "={id=" + id + "}";
    }

    public static String conditionRef(String name, String refName, String value) {
        return name + "={" + condition(refName, value) + "}";
    }

    public static String conditionRefEmpty(String name) {
        return name + "={null}";
    }

    public static String conditionNot(String condition) {
        return "!" + condition;
    }

    public static String condition(String name, String value) {
        return name + "='" + escapeQueryValue(value) + "'";
    }

    public static String condition(String name, int value) {
        return name + "=" + value;
    }

    public static String conditionEmpty(String name) {
        return name + "=null";
    }

    public static String condition(String name, long value) {
        return name + "=" + value;
    }

    public static String condition(String name, boolean value) {
        return name + "=" + value;
    }

    public static String orConditions(String... conditions) {
        return "(" + Arrays.stream(conditions).map(c -> "(" + c + ")").collect(Collectors.joining("||")) + ")";
    }

    public static String conditionIn(String name, Collection<?> ids, boolean isNumber) {
        if (isNumber) {
            return name + " IN " + SdkStringUtils.join(ids, ",");
        } else {
            //wrap values with '
            return name + " IN '" + SdkStringUtils.join(ids, "','") + "'";
        }
    }

    private static String escapeQueryValue(String value) {
        return value.replaceAll("(\\\\)", "$1$1").replaceAll("([\"'])", "\\\\$1");
    }
}

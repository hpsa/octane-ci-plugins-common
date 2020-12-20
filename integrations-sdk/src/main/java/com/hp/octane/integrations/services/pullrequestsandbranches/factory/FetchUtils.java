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
 */

package com.hp.octane.integrations.services.pullrequestsandbranches.factory;

import com.hp.octane.integrations.utils.SdkStringUtils;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FetchUtils {

    /**
     * This function build compiled patters
     *
     * @param patterns list of patterns, separated by the '|' character. The wildcard '*' can be used. Example: master|dev*branch)
     * @return list of patterns
     */
    public static List<Pattern> buildPatterns(String patterns) {
        List<Pattern> compiledPatterns = new LinkedList<>();
        if (SdkStringUtils.isNotEmpty(patterns)) {
            String[] patternsArr = patterns.split("[|]");
            for (String str : patternsArr) {
                if (!str.trim().isEmpty()) {
                    compiledPatterns.add(Pattern.compile(str.trim().replace(".", "\\.").replace("*", ".*")));
                }
            }
        }

        return compiledPatterns;
    }

    public static boolean isBranchMatch(List<Pattern> patterns, String branch) {

        if (patterns.isEmpty()) {
            return true;
        } else {
            for (Pattern pattern : patterns) {
                if (pattern.matcher(branch).find()) {
                    return true;
                }
            }
            return false;
        }
    }


    /**
     * Parse ISO8601DateString (format:YYYY-MM-DDTHH:MM:SSZ) to long
     * @param dateStr
     * @return
     */
    public static Long convertISO8601DateStringToLong(String dateStr) {
        if (dateStr == null || dateStr.isEmpty()) {
            return null;
        }
        //All timestamps return in ISO 8601 format:YYYY-MM-DDTHH:MM:SSZ
        return Instant.parse(dateStr).getEpochSecond() * 1000;
    }

    /**
     * return in ISO 8601 format:YYYY-MM-DDTHH:MM:SSZ
     * @param date
     * @return
     */
    public static String convertLongToISO8601DateString(long date) {
        return Instant.ofEpochMilli(date).toString();
    }

    /***
     * https://github.houston.softwaregrp.net/Octane/syncx.git=>Octane/syncx.git
     */
    public static String getRepoShortName(String url){
        String patternStr = "^.*[/:](.*/.*)$";
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(url);
        if (matcher.find() && matcher.groupCount() == 1) {
            return matcher.group(1);
        } else {
            return url;
        }
    }

}

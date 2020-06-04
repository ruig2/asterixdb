/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.util;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;

public class ParseUtil {

    private static final SourceLocation SRC_LOC = new SourceLocation(-1, -1);

    private ParseUtil() {
    }

    public static void warn(IWarningCollector warningCollector, String dataSourceName, long lineNum, int fieldNum,
            String warnMessage) {
        warningCollector.warn(
                Warning.forHyracks(SRC_LOC, ErrorCode.PARSING_ERROR, dataSourceName, lineNum, fieldNum, warnMessage));
    }
}
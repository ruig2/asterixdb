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

package org.apache.asterix.metadata.entities.fulltext;

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_NAME_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_STOPWORD_LIST_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FULLTEXT_ENTITY_ARECORD_USED_BY_FT_CONFIGS_FIELD_INDEX;

import java.util.List;

import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;

import com.google.common.collect.ImmutableList;

public class StopwordFullTextFilter extends AbstractFullTextFilter {
    ImmutableList<String> stopwordList;

    public StopwordFullTextFilter(String name, ImmutableList<String> stopwordList) {
        super(name, FullTextFilterType.STOPWORD);
        this.stopwordList = stopwordList;
    }

    public static StopwordFullTextFilter createFilterFromARecord(ARecord aRecord) {
        String name = ((AString) aRecord.getValueByPos(FULLTEXT_ENTITY_ARECORD_FULLTEXT_ENTITY_NAME_FIELD_INDEX))
                .getStringValue();
        ImmutableList.Builder stopwordsBuilder = ImmutableList.<String> builder();
        IACursor stopwordsCursor =
                ((AOrderedList) (aRecord.getValueByPos(FULLTEXT_ENTITY_ARECORD_STOPWORD_LIST_FIELD_INDEX))).getCursor();
        while (stopwordsCursor.next()) {
            stopwordsBuilder.add(((AString)stopwordsCursor.get()).getStringValue());
        }
        StopwordFullTextFilter filter = new StopwordFullTextFilter(name, stopwordsBuilder.build());

        IACursor usedByConfigsCursor =
                ((AOrderedList) (aRecord.getValueByPos(FULLTEXT_ENTITY_ARECORD_USED_BY_FT_CONFIGS_FIELD_INDEX)))
                        .getCursor();
        while (usedByConfigsCursor.next()) {
            filter.usedByFTConfigs.add(((AString) usedByConfigsCursor.get()).getStringValue());
        }

        return filter;
    }

    public List<String> getStopwordList() {
        return stopwordList;
    }
}

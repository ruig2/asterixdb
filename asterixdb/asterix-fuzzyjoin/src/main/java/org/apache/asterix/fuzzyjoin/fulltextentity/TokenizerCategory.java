package org.apache.asterix.fuzzyjoin.fulltextentity;

import org.apache.commons.lang3.EnumUtils;

public enum TokenizerCategory {
    NGRAM,
    WORD;

    public static TokenizerCategory fromString(String str) {
        return EnumUtils.getEnumIgnoreCase(TokenizerCategory.class, str);
    }
}

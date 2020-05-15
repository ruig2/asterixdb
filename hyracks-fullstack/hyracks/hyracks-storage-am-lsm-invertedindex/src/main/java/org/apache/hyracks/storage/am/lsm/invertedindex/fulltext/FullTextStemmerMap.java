package org.apache.hyracks.storage.am.lsm.invertedindex.fulltext;

import com.google.common.collect.ImmutableMap;
import opennlp.tools.stemmer.snowball.SnowballStemmer;

import java.util.Map;

public class FullTextStemmerMap {
    // ToDo: is the snowball stemmer thread safe?
    public static Map<AbstractStemmerFullTextFilter.StemmerLanguage, SnowballStemmer> STEMMER_MAP =
            ImmutableMap.of(
                    AbstractStemmerFullTextFilter.StemmerLanguage.ENGLISH, new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH)
            );
}

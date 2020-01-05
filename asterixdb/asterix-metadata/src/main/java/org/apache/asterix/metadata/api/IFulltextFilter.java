package org.apache.asterix.metadata.api;

import java.util.List;

// in progress...

public interface IFulltextFilter extends IFulltextEntity {

    enum FulltextFilterKind {
        // Assume the number of filter types are less than 2^8 = 256
        // When serializing the filter, only 8 bits will be reserved for the filter type
        // And don't change the existing value of the enums because this may corrupt the programs with older versions
        STOPWORD((byte)0),
        SYNONYM((byte)1);

        private final byte id;
        FulltextFilterKind(byte id) {
            this.id = id;
        }

        public byte getId() {
            return this.id;
        }
    }

    FulltextFilterKind getFilterKind();
    List<String> getUsedByFTConfigs();
    void addUsedByFTConfigs(String ftConfigName);
}

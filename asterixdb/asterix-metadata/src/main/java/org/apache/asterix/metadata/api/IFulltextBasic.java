package org.apache.asterix.metadata.api;

public interface IFulltextBasic {
    enum FulltextCategory {
        FULLTEXT_FILTER( (byte)0 ),
        FULLTEXT_CONFIG( (byte)1 );

        private byte id;
        FulltextCategory(byte id) {
            this.id = id;
        }

        public byte getId() {
            return this.id;
        }
    }



    FulltextCategory getCategory();
    String getName();
}

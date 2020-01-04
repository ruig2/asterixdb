package org.apache.asterix.metadata.api;

public interface IFulltextBasic {
    enum FulltextCategory {
        // How to show the enum name as a string in the result of a SQLPP query?
        // Current it is showned as a byte in the SQLPP terminal
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

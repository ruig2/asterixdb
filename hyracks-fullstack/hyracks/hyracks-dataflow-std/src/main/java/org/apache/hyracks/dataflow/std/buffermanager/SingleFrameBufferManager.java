package org.apache.hyracks.dataflow.std.buffermanager;

import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.nio.ByteBuffer;

public class SingleFrameBufferManager implements ISimpleFrameBufferManager {
    boolean isAcquired = false;
    ByteBuffer buffer = null;

    @Override public ByteBuffer acquireFrame(int frameSize) throws HyracksDataException {
        if (buffer == null) {
            buffer = ByteBuffer.allocate(frameSize);
        }

        if (isAcquired) {
            return null;
        } else {
            if (buffer.capacity() >= frameSize) {
                isAcquired = true;
                return (ByteBuffer) buffer.clear();
            } else {
                throw new HyracksDataException("Frame size changed");
            }
        }
    }

    @Override public void releaseFrame(ByteBuffer frame) {
        isAcquired = false;
        buffer.clear();
    }
}

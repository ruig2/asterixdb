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
package org.apache.hyracks.api.com.job.profiling.counters;

import org.apache.hyracks.api.dataflow.IPassableTimer;

public class TimeCounter extends Counter implements IPassableTimer {
    private long stopWatchStartTime;
    private final long INIT_VALUE = -1;

    private long frameWriterOpenTime, frameWriterCloseTime, frameWriterFirstFrameTime, frameWriterLastFrameTime;

    public TimeCounter(String name) {
        super(name);

        stopWatchStartTime = INIT_VALUE;
        frameWriterOpenTime = INIT_VALUE;
        frameWriterCloseTime = INIT_VALUE;
        frameWriterFirstFrameTime = INIT_VALUE;
        frameWriterLastFrameTime = INIT_VALUE;
    }

    public boolean isCounting() {
        return stopWatchStartTime > INIT_VALUE;
    }

    public void setOpenTimeIfNotSet(long startTime) {
        if (frameWriterOpenTime <= INIT_VALUE) {
            frameWriterOpenTime = startTime;
        }
    }

    public long getFrameWriterOpenTime() {
        return frameWriterOpenTime;
    }

    public void setCloseTimeIfLater(long closeTime) {
        if (frameWriterCloseTime < closeTime) {
            frameWriterCloseTime = closeTime;
        }
    }

    public long getFrameWriterCloseTime() {
        return frameWriterCloseTime;
    }

    public void setFirstFrameTimeIfNotSet(long firstFrameTime) {
        if (frameWriterFirstFrameTime <= INIT_VALUE) {
            frameWriterFirstFrameTime = firstFrameTime;
        }
    }

    public long getFrameWriterFirstFrameTime() {
        return frameWriterFirstFrameTime;
    }

    public void setLastFrameTimeIfLater(long lastFrameTime) {
        if (frameWriterLastFrameTime < lastFrameTime) {
            frameWriterLastFrameTime = lastFrameTime;
        }
    }

    public long getFrameWriterLastFrameTime() {
        return frameWriterLastFrameTime;
    }

    @Override
    public void pause() {
        if (stopWatchStartTime > INIT_VALUE) {
            update(System.nanoTime() - stopWatchStartTime);
            stopWatchStartTime = INIT_VALUE;
        }
    }

    @Override
    public void resume() {
        if (stopWatchStartTime <= INIT_VALUE) {
            stopWatchStartTime = System.nanoTime();
        }
    }

}

/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.join;

import io.crate.core.collections.Row;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.SingleUpstreamRowDownstream;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class NestedLoopOperation implements RowUpstream {

    private final static ESLogger LOGGER = Loggers.getLogger(NestedLoopOperation.class);

    private final CombinedRow combinedRow = new CombinedRow();
    private final LeftDownstreamHandle leftDownstreamHandle;
    private final RightDownstreamHandle rightDownstreamHandle;
    private final Object mutex = new Object();

    private SingleUpstreamRowDownstream leftDownstream;
    private SingleUpstreamRowDownstream rightDownstream;

    private RowDownstreamHandle downstream;
    private volatile boolean leftFinished = false;
    private volatile boolean rightFinished = false;
    private volatile boolean downstreamWantsMore = true;


    public NestedLoopOperation() {
        leftDownstreamHandle = new LeftDownstreamHandle();
        rightDownstreamHandle = new RightDownstreamHandle();
        leftDownstream = new SingleUpstreamRowDownstream(leftDownstreamHandle);
        rightDownstream = new SingleUpstreamRowDownstream(rightDownstreamHandle);
    }

    public RowDownstream leftDownStream() {
        return leftDownstream;
    }

    public RowDownstream rightDownStream() {
        return rightDownstream;
    }

    public void downstream(RowDownstream downstream) {
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public void pause() {
        leftDownstream.upstream().pause();
        rightDownstream.upstream().pause();
    }

    @Override
    public void resume(boolean async) {
        leftDownstream.upstream().resume(async);
        rightDownstream.upstream().resume(async);
    }

    @Override
    public void repeat() {
        throw new UnsupportedOperationException();
    }

    static class CombinedRow implements Row {

        Row outerRow;
        Row innerRow;

        @Override
        public int size() {
            return outerRow.size() + innerRow.size();
        }

        @Override
        public Object get(int index) {
            if (index < outerRow.size()) {
                return outerRow.get(index);
            }
            return innerRow.get(index - outerRow.size());
        }

        @Override
        public Object[] materialize() {
            Object[] left = outerRow.materialize();
            Object[] right = innerRow.materialize();

            Object[] newRow = new Object[left.length + right.length];
            System.arraycopy(left, 0, newRow, 0, left.length);
            System.arraycopy(right, 0, newRow, left.length, right.length);
            return newRow;
        }

        @Override
        public String toString() {
            return "CombinedRow{" +
                    " outer=" + outerRow +
                    ", inner=" + innerRow +
                    '}';
        }
    }

    private class LeftDownstreamHandle implements RowDownstreamHandle {

        private Row lastRow;

        @Override
        public boolean setNextRow(Row row) {
            LOGGER.trace("left downstream received a row {}", row);

            synchronized (mutex) {
                if (rightFinished && (!rightDownstreamHandle.receivedRows || !downstreamWantsMore)) {
                    return false;
                }
                lastRow = row;
                leftDownstream.upstream().pause();
                rightDownstreamHandle.leftIsPaused = true;
            }
            return rightDownstreamHandle.resume();
        }

        @Override
        public void finish() {
            synchronized (mutex) {
                leftFinished = true;
                if (rightFinished) {
                    downstream.finish();
                } else {
                    rightDownstream.upstream().resume(false);
                }
            }
            LOGGER.debug("left downstream finished");
        }

        @Override
        public void fail(Throwable throwable) {
            downstream.fail(throwable);
        }

    }

    private class RightDownstreamHandle implements RowDownstreamHandle {

        Row lastRow = null;

        boolean receivedRows = false;
        boolean leftIsPaused = false;

        public RightDownstreamHandle() {
        }

        @Override
        public boolean setNextRow(final Row rightRow) {
            LOGGER.trace("right downstream received a row {}", rightRow);
            receivedRows = true;


            if (leftIsPaused) {
                return emitRow(rightRow);
            }

            synchronized (mutex) {
                if (leftDownstreamHandle.lastRow == null) {
                    if (leftFinished) {
                        return false;
                    }
                    lastRow = rightRow;
                    rightDownstream.upstream().pause();
                    return true;
                }
            }
            return emitRow(rightRow);
        }

        private boolean emitRow(Row row) {
            combinedRow.outerRow = leftDownstreamHandle.lastRow;
            combinedRow.innerRow = row;
            boolean wantsMore = downstream.setNextRow(combinedRow);
            downstreamWantsMore = wantsMore;
            return wantsMore;
        }

        @Override
        public void finish() {
            synchronized (mutex) {
                rightFinished = true;
                if (leftFinished) {
                    downstream.finish();
                } else {
                    leftDownstream.upstream().resume(false);
                }
            }
        }

        @Override
        public void fail(Throwable throwable) {
            downstream.fail(throwable);
        }

        public boolean resume() {
            if (lastRow != null) {
                boolean wantMore = emitRow(lastRow);
                if (!wantMore) {
                    return false;
                }
                lastRow = null;
            }

            if (rightFinished) {
                if (receivedRows) {
                    rightDownstream.upstream().repeat();
                } else {
                    return false;
                }
            } else {
                rightDownstream.upstream().resume(false);
            }
            return true;
        }
    }
}

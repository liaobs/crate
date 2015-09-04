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

package io.crate.operation.projectors;

import com.google.common.collect.Ordering;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.sorting.OrderingByPosition;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingSortingRowDownstream implements Projector  {

    private final Ordering<Row> ordering;
    private final List<BlockingSortingRowDownstreamHandle> downstreamHandles = new ArrayList<>();
    private final List<RowUpstream> upstreams = new ArrayList<>();
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicBoolean downstreamAborted = new AtomicBoolean(false);
    private final LowestCommon lowestCommon = new LowestCommon();
    private RowDownstreamHandle downstreamContext;

    public BlockingSortingRowDownstream(int[] orderBy,
                                        boolean[] reverseFlags,
                                        Boolean[] nullsFirst) {
        List<Comparator<Row>> comparators = new ArrayList<>(orderBy.length);
        for (int i = 0; i < orderBy.length; i++) {
            comparators.add(OrderingByPosition.rowOrdering(orderBy[i], reverseFlags[i], nullsFirst[i]));
        }
        ordering = Ordering.compound(comparators);
    }

    @Override
    public void startProjection(ExecutionState executionState) {
        if (remainingUpstreams.get() == 0) {
            upstreamFinished();
        }
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        upstreams.add(upstream);
        remainingUpstreams.incrementAndGet();
        BlockingSortingRowDownstreamHandle handle = new BlockingSortingRowDownstreamHandle(this, upstream);
        downstreamHandles.add(handle);
        return handle;
    }

    @Override
    public void downstream(RowDownstream downstream) {
        downstreamContext = downstream.registerUpstream(this);
    }

    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            if (downstreamContext != null) {
                downstreamContext.finish();
            }
        }
    }

    protected void upstreamFailed(Throwable throwable) {
        downstreamAborted.compareAndSet(false, true);
        if (remainingUpstreams.decrementAndGet() == 0) {
            if (downstreamContext != null) {
                downstreamContext.fail(throwable);
            }
        }
    }

    @Override
    public void pause() {
        for (BlockingSortingRowDownstreamHandle handle : downstreamHandles) {
            if (!handle.isFinished()) {
                handle.upstream.pause();
            }
        }
    }

    @Override
    public void resume(boolean async) {
        for (BlockingSortingRowDownstreamHandle handle : downstreamHandles) {
            if (!handle.isFinished()) {
                handle.upstream.resume(async);
            }
        }
    }

    public class BlockingSortingRowDownstreamHandle implements RowDownstreamHandle {

        private final BlockingSortingRowDownstream projector;
        private final RowUpstream upstream;
        private AtomicBoolean finished = new AtomicBoolean(false);
        private Row row = null;
        private final Object lock = new Object();

        public BlockingSortingRowDownstreamHandle(BlockingSortingRowDownstream projector, RowUpstream upstream) {
            this.upstream = upstream;
            this.projector = projector;
        }

        private AtomicBoolean pendingPause = new AtomicBoolean(false);

        @Override
        public boolean setNextRow(Row row) {
            if (projector.downstreamAborted.get()) {
                return false;
            }
            if (!lowestCommon.isEmittable(row, this)) {
                pause();
            }
            return downstreamContext.setNextRow(row);
        }

        public Row row() {
            return row;
        }

        public boolean isFinished() {
            return finished.get();
        }

        @Override
        public void fail(Throwable throwable) {
            projector.upstreamFailed(throwable);
        }

        private void pause() {
            synchronized (lock) {
                if (pendingPause.compareAndSet(true, false)) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

        private void resume() {
            synchronized (lock) {
                if (!pendingPause.getAndSet(false)) {
                    lock.notify();
                }
            }
        }


        @Override
        public void finish() {
            if (finished.compareAndSet(false, true)) {
                projector.upstreamFinished();
                lowestCommon.raiseLowest(null, this);
            }
        }
    }

    private class LowestCommon {

        private Row lowestToEmit = null;
        private final Object lowestLock = new Object();

        public boolean isEmittable(Row row, BlockingSortingRowDownstreamHandle handle) {
            if (lowestToEmit != null && (lowestToEmit == row || ordering.compare(row, lowestToEmit) >= 0)) {
                return true;
            }
            return raiseLowest(row, handle);
        }

        public boolean raiseLowest(Row row, final BlockingSortingRowDownstreamHandle handle) {
            synchronized (lowestLock) {
                for (BlockingSortingRowDownstreamHandle h : downstreamHandles) {
                    if (h != handle && h.row() == null && !h.isFinished()) {
                        handle.row = row;
                        handle.pendingPause.set(true);
                        return false;
                    }
                }
                Row nextLowest = row;
                Set<BlockingSortingRowDownstreamHandle> toContinue = new HashSet() {{
                    add(handle);
                }};
                for (BlockingSortingRowDownstreamHandle h : downstreamHandles) {
                    if (nextLowest == null) {
                        nextLowest = h.row();
                        toContinue.clear();
                        toContinue.add(h);
                        continue;
                    }
                    // h != handle check if this is the only handle
                    if (h != handle) {
                        int comp = ordering.compare(h.row(), nextLowest);
                        if (comp == 0) {
                            toContinue.add(h);
                        } else if (comp > 0) {
                            toContinue.clear();
                            toContinue.add(h);
                            nextLowest = h.row();
                        }
                    }
                }
                if (nextLowest != null && nextLowest != lowestToEmit) {
                    boolean cont = nextLowest == row;
                    lowestToEmit = new RowN(nextLowest.materialize());
                    for (BlockingSortingRowDownstreamHandle h : toContinue) {
                        if (h != handle) {
                            h.row = null;
                            h.resume();
                        }
                    }
                    if (!cont && !handle.isFinished()) {
                        handle.row = row;
                        handle.pendingPause.set(true);
                    }
                    return cont;
                }
                handle.row = row;
                handle.pendingPause.set(true);
                return false;
            }
        }

    }
}

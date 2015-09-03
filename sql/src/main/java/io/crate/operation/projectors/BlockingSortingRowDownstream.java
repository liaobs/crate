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
import org.apache.commons.lang3.RandomUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

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

    private static ESLogger LOGGER = Loggers.getLogger(MergeProjector.class);


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
            ////////////////LOGGER.error("total finish!");
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

        private String ident = ""+RandomUtils.nextInt(0, 100);

        private final Object lock = new Object();

        public BlockingSortingRowDownstreamHandle(BlockingSortingRowDownstream projector, RowUpstream upstream) {
            //LOGGER.error("{} created on proj {}", this.ident, projector);
            this.upstream = upstream;
            this.projector = projector;
        }

        private AtomicBoolean pendingPause = new AtomicBoolean(false);

        @Override
        public boolean setNextRow(Row row) {
            ////////LOGGER.error("{} setNextRow", this.ident);
            row = new RowN(row.materialize());
            if (projector.downstreamAborted.get()) {
                return false;
            }
            if (!lowestCommon.isEmittable(row, this)) {
                pendingPause.set(true);
                pause();
            }
            //////LOGGER.error("{} emit", ident);
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
            if (pendingPause.compareAndSet(true, false)) {
                synchronized (lock) {
                    try {
                        //LOGGER.error("{} pause", this.ident);
                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                //////LOGGER.error("{} not pausing, conflict", this.ident);
            }

        }

        private void resume() {
            if (!pendingPause.getAndSet(false)) {
                synchronized (lock) {
                    lock.notify();
                }
                //LOGGER.error("{} resume", this.ident);
            } else {
                //////LOGGER.error("{} not resuming, just abort pause", this.ident);
            }
        }


        @Override
        public void finish() {
            if (finished.compareAndSet(false, true)) {
                projector.upstreamFinished();
                lowestCommon.raiseLowest(null, this);
                //LOGGER.error("{} finish()", this.ident);
            }
        }
    }

    private class LowestCommon {

        private Row lowestToEmit = null;

        public boolean isEmittable(Row row, BlockingSortingRowDownstreamHandle handle) {
            ////LOGGER.error("{} isEmittable ?", handle.ident);
            if( lowestToEmit != null && ( lowestToEmit == row || ordering.compare(row, lowestToEmit) >= 0)) {
                ////LOGGER.error("{} is emittable", handle.ident);
                return true;
            } else {
                return raiseLowest(row, handle);
            }
        }

        public synchronized boolean raiseLowest(Row row, final BlockingSortingRowDownstreamHandle handle) {
            ////LOGGER.error("{} raiseLowest", handle.ident);
            Row nextLowest = row;
            Set<BlockingSortingRowDownstreamHandle> toContinue = new HashSet(){{add(handle);}};
            for (BlockingSortingRowDownstreamHandle h : downstreamHandles) {
                if (h != handle && h.row() == null && !h.isFinished()) {
                    handle.row = row;
                    return false;
                }
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
                lowestToEmit = nextLowest;
                ////////LOGGER.error("{} raised", handle.ident);
                for (BlockingSortingRowDownstreamHandle h : toContinue) {
                    if ( h != handle) {
                        //LOGGER.error("{} resume {}", handle.ident, h.ident);
                        h.row = null;
                        h.resume();
                    }
                }
                boolean cont = lowestToEmit == row;
                if (!cont) {
                    handle.row = row;
                }
                return cont;
            }
            ////////LOGGER.error("{} finished without raising", handle.ident);
            handle.row = row;
            return false;
        }

    }
}

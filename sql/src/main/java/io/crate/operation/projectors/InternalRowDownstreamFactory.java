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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.Streamer;
import io.crate.executor.transport.distributed.*;
import io.crate.operation.NodeOperation;
import io.crate.operation.RowDownstream;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.StreamerVisitor;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

@Singleton
public class InternalRowDownstreamFactory implements RowDownstreamFactory {

    private final ClusterService clusterService;
    private final TransportDistributedResultAction transportDistributedResultAction;

    @Inject
    public InternalRowDownstreamFactory(ClusterService clusterService,
                                        TransportDistributedResultAction transportDistributedResultAction) {
        this.clusterService = clusterService;
        this.transportDistributedResultAction = transportDistributedResultAction;
    }

    public RowDownstream createDownstream(NodeOperation nodeOperation,
                                          DistributionType distributionType,
                                          UUID jobId,
                                          int pageSize) {
        Streamer<?>[] streamers = StreamerVisitor.streamerFromOutputs(nodeOperation.executionPhase());
        Collection<String> downstreamNodeIds = nodeOperation.downstreamNodes();
        assert !ExecutionPhases.hasDirectResponseDownstream(downstreamNodeIds);
        assert downstreamNodeIds.size() > 0 : "must have at least one downstream";

        // TODO: set bucketIdx properly
        ArrayList<String> server = Lists.newArrayList(nodeOperation.executionPhase().executionNodes());
        Collections.sort(server);
        String localNodeId = clusterService.localNode().id();
        int bucketIdx = Math.max(server.indexOf(localNodeId), 0);

        MultiBucketBuilder multiBucketBuilder;

        // TODO: optimize this to by-pass transport / paging and access RowDownstream of the target-context directly
        // if it is on the same node
        switch (distributionType) {
            case MODULO:
                if (downstreamNodeIds.size() == 1) {
                    multiBucketBuilder = new BroadcastingBucketBuilder(streamers, downstreamNodeIds.size());
                } else {
                    multiBucketBuilder = new ModuloBucketBuilder(streamers, downstreamNodeIds.size());
                }
                break;
            case BROADCAST:
                multiBucketBuilder = new BroadcastingBucketBuilder(streamers, downstreamNodeIds.size());
                break;
            case SAME_NODE:
                bucketIdx = 0;
                multiBucketBuilder = new BroadcastingBucketBuilder(streamers, 1);
                downstreamNodeIds = ImmutableList.of(localNodeId);
                break;
            default:
                throw new UnsupportedOperationException("Can't handle distributionType: " + distributionType);
        }

        return new DistributingDownstream(
                jobId,
                multiBucketBuilder,
                nodeOperation.downstreamExecutionPhaseId(),
                nodeOperation.downstreamExecutionPhaseInputId(),
                bucketIdx,
                downstreamNodeIds,
                transportDistributedResultAction,
                streamers,
                pageSize
        );

    }
}

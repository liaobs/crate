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

package io.crate.planner.projection;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FetchProjection extends Projection {

    public static final ProjectionFactory<FetchProjection> FACTORY = new ProjectionFactory<FetchProjection>() {
        @Override
        public FetchProjection newInstance() {
            return new FetchProjection();
        }
    };

    private int executionPhaseId;
    private Symbol docIdSymbol;
    private List<Symbol> inputSymbols;
    private List<Symbol> outputSymbols;
    private List<ReferenceInfo> partitionBy;
    private Set<String> executionNodes;
    private IntObjectOpenHashMap<String> jobSearchContextIdToNode;
    private IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard;

    private FetchProjection() {
    }

    public FetchProjection(int executionPhaseId,
                           Symbol docIdSymbol,
                           List<Symbol> inputSymbols,
                           List<Symbol> outputSymbols,
                           List<ReferenceInfo> partitionBy,
                           Set<String> executionNodes,
                           IntObjectOpenHashMap<String> jobSearchContextIdToNode,
                           IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard) {
        this.executionPhaseId = executionPhaseId;
        this.docIdSymbol = docIdSymbol;
        this.inputSymbols = inputSymbols;
        this.outputSymbols = outputSymbols;
        this.partitionBy = partitionBy;
        this.executionNodes = executionNodes;
        this.jobSearchContextIdToNode = jobSearchContextIdToNode;
        this.jobSearchContextIdToShard = jobSearchContextIdToShard;
    }

    public int executionPhaseId() {
        return executionPhaseId;
    }

    public Symbol docIdSymbol() {
        return docIdSymbol;
    }

    public List<Symbol> inputSymbols() {
        return inputSymbols;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public List<ReferenceInfo> partitionedBy() {
        return partitionBy;
    }

    public Set<String> executionNodes() {
        return executionNodes;
    }

    public IntObjectOpenHashMap<String> jobSearchContextIdToNode() {
        return jobSearchContextIdToNode;
    }

    public IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard() {
        return jobSearchContextIdToShard;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.FETCH;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitFetchProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputSymbols;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchProjection that = (FetchProjection) o;

        if (executionPhaseId != that.executionPhaseId) return false;
        if (executionNodes != that.executionNodes) return false;
        if (!docIdSymbol.equals(that.docIdSymbol)) return false;
        if (!inputSymbols.equals(that.inputSymbols)) return false;
        if (!outputSymbols.equals(that.outputSymbols)) return false;
        if (!outputSymbols.equals(that.outputSymbols)) return false;
        return partitionBy.equals(that.partitionBy);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + executionPhaseId;
        result = 31 * result + docIdSymbol.hashCode();
        result = 31 * result + inputSymbols.hashCode();
        result = 31 * result + outputSymbols.hashCode();
        result = 31 * result + partitionBy.hashCode();
        result = 31 * result + executionNodes.hashCode();
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        executionPhaseId = in.readVInt();
        docIdSymbol = Symbol.fromStream(in);
        int inputSymbolsSize = in.readVInt();
        inputSymbols = new ArrayList<>(inputSymbolsSize);
        for (int i = 0; i < inputSymbolsSize; i++) {
            inputSymbols.add(Symbol.fromStream(in));
        }
        int outputSymbolsSize = in.readVInt();
        outputSymbols = new ArrayList<>(outputSymbolsSize);
        for (int i = 0; i < outputSymbolsSize; i++) {
            outputSymbols.add(Symbol.fromStream(in));
        }
        int partitionedBySize = in.readVInt();
        partitionBy = new ArrayList<>(partitionedBySize);
        for (int i = 0; i < partitionedBySize; i++) {
            ReferenceInfo referenceInfo = new ReferenceInfo();
            referenceInfo.readFrom(in);
            partitionBy.add(referenceInfo);
        }
        int executionNodesSize = in.readVInt();
        executionNodes = new HashSet<>(executionNodesSize);
        for (int i = 0; i < executionNodesSize; i++) {
            executionNodes.add(in.readString());
        }

        int numJobSearchContextIdToNode = in.readVInt();
        jobSearchContextIdToNode = new IntObjectOpenHashMap<>(numJobSearchContextIdToNode);
        for (int i = 0; i < numJobSearchContextIdToNode; i++) {
            jobSearchContextIdToNode.put(in.readVInt(), in.readString());
        }
        int numJobSearchContextIdToShard = in.readVInt();
        jobSearchContextIdToShard = new IntObjectOpenHashMap<>(numJobSearchContextIdToShard);
        for (int i = 0; i < numJobSearchContextIdToShard; i++) {
            jobSearchContextIdToShard.put(in.readVInt(), ShardId.readShardId(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(executionPhaseId);
        Symbol.toStream(docIdSymbol, out);
        out.writeVInt(inputSymbols.size());
        for (Symbol symbol : inputSymbols) {
            Symbol.toStream(symbol, out);
        }
        out.writeVInt(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            Symbol.toStream(symbol, out);
        }
        out.writeVInt(partitionBy.size());
        for (ReferenceInfo referenceInfo : partitionBy) {
            referenceInfo.writeTo(out);
        }
        out.writeVInt(executionNodes.size());
        for (String nodeId : executionNodes) {
            out.writeString(nodeId);
        }

        out.writeVInt(jobSearchContextIdToNode.size());
        for (IntObjectCursor<String> entry : jobSearchContextIdToNode) {
            out.writeVInt(entry.key);
            out.writeString(entry.value);
        }
        out.writeVInt(jobSearchContextIdToShard.size());
        for (IntObjectCursor<ShardId> entry : jobSearchContextIdToShard) {
            out.writeVInt(entry.key);
            entry.value.writeTo(out);
        }
    }
}

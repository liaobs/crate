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

package io.crate.integrationtests;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Streamer;
import io.crate.action.job.ContextPreparer;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.WhereClause;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.NodeFetchRequest;
import io.crate.executor.transport.NodeFetchResponse;
import io.crate.executor.transport.TransportExecutor;
import io.crate.executor.transport.TransportFetchNodeAction;
import io.crate.executor.transport.distributed.SingleBucketBuilder;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.NodeOperation;
import io.crate.operation.fetch.RowInputSymbolVisitor;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.RowGranularity;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.ConsumingPlanner;
import io.crate.planner.consumer.QueryThenFetchConsumer;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.StreamerVisitor;
import io.crate.planner.node.dql.CollectAndMerge;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class FetchOperationIntegrationTest extends SQLTransportIntegrationTest {

    Setup setup = new Setup(sqlExecutor);
    TransportExecutor executor;
    DocSchemaInfo docSchemaInfo;
    ConsumingPlanner consumingPlanner;

    @Before
    public void transportSetUp() {
        executor = internalCluster().getInstance(TransportExecutor.class);
        docSchemaInfo = internalCluster().getInstance(DocSchemaInfo.class);
        consumingPlanner = internalCluster().getInstance(ConsumingPlanner.class);
    }

    @After
    public void transportTearDown() {
        executor = null;
        docSchemaInfo = null;
    }

    private void setUpCharacters() {
        sqlExecutor.exec("create table characters (id int primary key, name string) " +
                "clustered into 2 shards with(number_of_replicas=0)");
        sqlExecutor.ensureYellowOrGreen();
        sqlExecutor.execBulk("insert into characters (id, name) values (?, ?)",
                new Object[][]{
                        new Object[]{1, "Arthur"},
                        new Object[]{2, "Ford"},
                }
        );
        sqlExecutor.refresh("characters");
    }

    private Plan analyzeAndPlan(String stmt) {
        Analysis analysis = analyze(stmt);
        Planner planner = internalCluster().getInstance(Planner.class);
        return planner.plan(analysis, UUID.randomUUID());
    }

    private Analysis analyze(String stmt) {
        Analyzer analyzer = internalCluster().getInstance(Analyzer.class);
        return analyzer.analyze(
                SqlParser.createStatement(stmt),
                new ParameterContext(new Object[0], new Object[0][], null)
        );
    }

    private CollectPhase createCollectNode(Planner.Context plannerContext, boolean keepContextForFetcher) {
        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");

        ReferenceInfo docIdRefInfo = tableInfo.getReferenceInfo(new ColumnIdent("_docid"));
        Symbol docIdRef = new Reference(docIdRefInfo);
        List<Symbol> toCollect = ImmutableList.of(docIdRef);

        CollectPhase collectNode = new CollectPhase(
                UUID.randomUUID(),
                plannerContext.nextExecutionPhaseId(),
                "collect",
                tableInfo.getRouting(WhereClause.MATCH_ALL, null),
                RowGranularity.DOC,
                toCollect,
                ImmutableList.<Projection>of(),
                WhereClause.MATCH_ALL,
                DistributionType.BROADCAST
        );
        collectNode.keepContextForFetcher(keepContextForFetcher);
        plannerContext.allocateJobSearchContextIds(collectNode.routing());

        return collectNode;
    }

    private List<Bucket> getBuckets(CollectPhase collectNode) throws InterruptedException, java.util.concurrent.ExecutionException, TimeoutException {
        List<Bucket> results = new ArrayList<>();
        for (String nodeName : internalCluster().getNodeNames()) {
            ContextPreparer contextPreparer = internalCluster().getInstance(ContextPreparer.class, nodeName);
            JobContextService contextService = internalCluster().getInstance(JobContextService.class, nodeName);

            NodeOperation nodeOperation = NodeOperation.withDownstream(collectNode, mock(ExecutionPhase.class), (byte) 0);
            Streamer<?>[] streamers = StreamerVisitor.streamerFromOutputs(nodeOperation.executionPhase());
            SingleBucketBuilder bucketBuilder = new SingleBucketBuilder(streamers);
            JobExecutionContext.Builder builder = contextService.newBuilder(collectNode.jobId());
            contextPreparer.prepare(collectNode.jobId(), nodeOperation, builder, bucketBuilder);

            JobExecutionContext context = contextService.createContext(builder);
            context.start();
            results.add(bucketBuilder.result().get(2, TimeUnit.SECONDS));
        }
        return results;
    }

    @Test
    public void testCollectDocId() throws Exception {
        setUpCharacters();
        Planner.Context plannerContext = newPlannerContext();
        CollectPhase collectNode = createCollectNode(plannerContext, false);

        List<Bucket> results = getBuckets(collectNode);

        assertThat(results.size(), is(2));
        int seenJobSearchContextId = -1;
        for (Bucket rows : results) {
            assertThat(rows.size(), is(1));
            Object docIdCol = rows.iterator().next().get(0);
            assertNotNull(docIdCol);
            assertThat(docIdCol, instanceOf(Long.class));
            long docId = (long)docIdCol;
            // unpack jobSearchContextId and reader doc id from docId
            int jobSearchContextId = (int)(docId >> 32);
            int doc = (int)docId;
            assertThat(doc, is(0));
            assertThat(jobSearchContextId, greaterThan(-1));
            if (seenJobSearchContextId == -1) {
                assertThat(jobSearchContextId, anyOf(is(0), is(1)));
                seenJobSearchContextId = jobSearchContextId;
            } else {
                assertThat(jobSearchContextId, is(seenJobSearchContextId == 0 ? 1 : 0));
            }
        }
    }

    @Test
    public void testFetchAction() throws Exception {
        setUpCharacters();

        Analysis analysis = analyze("select id, name from characters");
        QueryThenFetchConsumer queryThenFetchConsumer = internalCluster().getInstance(QueryThenFetchConsumer.class);
        Planner.Context plannerContext = newPlannerContext();
        ConsumerContext consumerContext = new ConsumerContext(analysis.rootRelation(), plannerContext);
        CollectAndMerge plan = (CollectAndMerge) queryThenFetchConsumer.consume(analysis.rootRelation(), consumerContext).plan();

        List<Bucket> results = getBuckets(plan.collectPhase());


        TransportFetchNodeAction transportFetchNodeAction = internalCluster().getInstance(TransportFetchNodeAction.class);

        // extract docIds by nodeId and jobSearchContextId
        Map<String, LongArrayList> jobSearchContextDocIds = new HashMap<>();
        for (Bucket rows : results) {
            long docId = (long)rows.iterator().next().get(0);
            // unpack jobSearchContextId and reader doc id from docId
            int jobSearchContextId = (int)(docId >> 32);
            String nodeId = plannerContext.nodeId(jobSearchContextId);
            LongArrayList docIdsPerNode = jobSearchContextDocIds.get(nodeId);
            if (docIdsPerNode == null) {
                docIdsPerNode = new LongArrayList();
                jobSearchContextDocIds.put(nodeId, docIdsPerNode);
            }
            docIdsPerNode.add(docId);
        }

        Iterable<Projection> projections = Iterables.filter(plan.localMerge().projections(), Predicates.instanceOf(FetchProjection.class));
        FetchProjection fetchProjection = (FetchProjection )Iterables.getOnlyElement(projections);
        RowInputSymbolVisitor rowInputSymbolVisitor = new RowInputSymbolVisitor(internalCluster().getInstance(Functions.class));
        RowInputSymbolVisitor.Context context = rowInputSymbolVisitor.extractImplementations(fetchProjection.outputSymbols());

        final CountDownLatch latch = new CountDownLatch(jobSearchContextDocIds.size());
        final List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, LongArrayList> nodeEntry : jobSearchContextDocIds.entrySet()) {
            NodeFetchRequest nodeFetchRequest = new NodeFetchRequest();
            nodeFetchRequest.jobId(plan.collectPhase().jobId());
            nodeFetchRequest.executionPhaseId(plan.collectPhase().executionPhaseId());
            nodeFetchRequest.toFetchReferences(context.references());
            nodeFetchRequest.jobSearchContextDocIds(nodeEntry.getValue());

            transportFetchNodeAction.execute(nodeEntry.getKey(), nodeFetchRequest, new ActionListener<NodeFetchResponse>() {
                @Override
                public void onResponse(NodeFetchResponse nodeFetchResponse) {
                    for (Row row : nodeFetchResponse.rows()) {
                        rows.add(row);
                    }
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    latch.countDown();
                    fail(e.getMessage());
                }
            });
        }
        latch.await();

        assertThat(rows.size(), is(2));
        for (Row row : rows) {
            assertThat((Integer) row.get(0), anyOf(is(1), is(2)));
            assertThat((BytesRef) row.get(1), anyOf(is(new BytesRef("Arthur")), is(new BytesRef("Ford"))));
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFetchProjection() throws Exception {
        setUpCharacters();

        Plan plan = analyzeAndPlan("select id, name, substr(name, 2) from characters order by id");
        assertThat(plan, instanceOf(CollectAndMerge.class));
        CollectAndMerge qtf = (CollectAndMerge) plan;

        assertThat(qtf.collectPhase().keepContextForFetcher(), is(true));
        assertThat(((FetchProjection) qtf.localMerge().projections().get(1)).jobSearchContextIdToNode(), notNullValue());
        assertThat(((FetchProjection) qtf.localMerge().projections().get(1)).jobSearchContextIdToShard(), notNullValue());

        Job job = executor.newJob(plan);
        ListenableFuture<List<TaskResult>> results = Futures.allAsList(executor.execute(job));

        final List<Object[]> resultingRows = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        Futures.addCallback(results, new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(List<TaskResult> resultList) {
                for (Row row : resultList.get(0).rows()) {
                    resultingRows.add(row.materialize());
                }
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
                fail(t.getMessage());
            }
        });

        latch.await();
        assertThat(resultingRows.size(), is(2));
        assertThat(resultingRows.get(0).length, is(3));
        assertThat((Integer) resultingRows.get(0)[0], is(1));
        assertThat((BytesRef) resultingRows.get(0)[1], is(new BytesRef("Arthur")));
        assertThat((BytesRef) resultingRows.get(0)[2], is(new BytesRef("rthur")));
        assertThat((Integer) resultingRows.get(1)[0], is(2));
        assertThat((BytesRef) resultingRows.get(1)[1], is(new BytesRef("Ford")));
        assertThat((BytesRef) resultingRows.get(1)[2], is(new BytesRef("ord")));
    }

    protected Planner.Context newPlannerContext() {
        return new Planner.Context(clusterService(), UUID.randomUUID(), consumingPlanner);
    }
}
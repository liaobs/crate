/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import io.crate.Constants;
import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CreateBlobTable;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class CreateBlobTableStatementAnalyzer extends BlobTableAnalyzer<CreateBlobTableAnalyzedStatement> {

    private static final TablePropertiesAnalyzer TABLE_PROPERTIES_ANALYZER = new TablePropertiesAnalyzer();
    private final Schemas schemas;

    @Inject
    public CreateBlobTableStatementAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    @Override
    public CreateBlobTableAnalyzedStatement visitCreateBlobTable(CreateBlobTable node, Analysis analysis) {
        CreateBlobTableAnalyzedStatement statement = new CreateBlobTableAnalyzedStatement();
        TableIdent tableIdent = tableToIdent(node.name());
        statement.table(tableIdent, schemas);

        if (node.clusteredBy().isPresent()) {
            ClusteredBy clusteredBy = node.clusteredBy().get();

            int numShards;
            if (clusteredBy.numberOfShards().isPresent()) {
                numShards = ExpressionToNumberVisitor.convert(clusteredBy.numberOfShards().get(),
                        analysis.parameterContext().parameters()).intValue();
                if (numShards < 1) {
                    throw new IllegalArgumentException("num_shards in CLUSTERED clause must be greater than 0");
                }
            } else {
                numShards = Constants.DEFAULT_NUM_SHARDS;
            }
            statement.tableParameter().settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards);
        }


        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        TABLE_PROPERTIES_ANALYZER.analyze(
                statement.tableParameter(), new BlobTableParameterInfo(),
                node.genericProperties(), analysis.parameterContext().parameters(), true);

        return statement;
    }
}

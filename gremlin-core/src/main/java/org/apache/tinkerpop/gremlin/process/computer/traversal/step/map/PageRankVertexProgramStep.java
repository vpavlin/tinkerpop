/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.computer.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TimesModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PageRankVertexProgramStep extends VertexProgramStep implements TraversalParent, ByModulating, TimesModulating {

    private transient Function<Graph, GraphComputer> graphComputerFunction = Graph::compute;

    private PureTraversal<Vertex, Edge> edgeTraversal;
    private String pageRankProperty = PageRankVertexProgram.PAGE_RANK;
    private int times = 30;
    private final double alpha;

    public PageRankVertexProgramStep(final Traversal.Admin traversal, final double alpha) {
        super(traversal);
        this.alpha = alpha;
        this.modulateBy(__.<Vertex>outE().asAdmin());
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> edgeTraversal) {
        this.edgeTraversal = new PureTraversal<>((Traversal.Admin<Vertex, Edge>) edgeTraversal);
        this.integrateChild(this.edgeTraversal.get());
    }

    @Override
    public void modulateBy(final String pageRankProperty) {
        this.pageRankProperty = pageRankProperty;
    }

    @Override
    public void modulateTimes(int times) {
        this.times = times;
    }

    @Override
    public List<Traversal.Admin<Vertex, Edge>> getLocalChildren() {
        return Collections.singletonList(this.edgeTraversal.get());
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.edgeTraversal.get(), this.pageRankProperty, this.times);
    }

    @Override
    public void setGraphComputerFunction(final Function<Graph, GraphComputer> graphComputerFunction) {
        this.graphComputerFunction = graphComputerFunction;
    }

    @Override
    public PageRankVertexProgram generateProgram(final Graph graph) {
        this.edgeTraversal.reset();
        final Traversal.Admin<Vertex, Edge> compiledTraversal = this.edgeTraversal.get();
        compiledTraversal.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
        return PageRankVertexProgram.build()
                .property(this.pageRankProperty)
                .iterations(this.times)
                .alpha(this.alpha)
                .edges(compiledTraversal)
                .create(graph);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalParent.super.getSelfAndChildRequirements();
    }

    @Override
    public GraphComputer generateComputer(final Graph graph) {
        return this.graphComputerFunction.apply(graph).persist(GraphComputer.Persist.EDGES).result(GraphComputer.ResultGraph.NEW);
    }

    @Override
    public PageRankVertexProgramStep clone() {
        final PageRankVertexProgramStep clone = (PageRankVertexProgramStep) super.clone();
        clone.edgeTraversal = this.edgeTraversal.clone();
        this.integrateChild(this.edgeTraversal.get());
        return clone;
    }
}

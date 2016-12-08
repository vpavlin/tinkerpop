/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.tinkergraph.process.akka;

import akka.actor.ActorSystem;
import akka.actor.Props;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.util.HashPartitioner;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.as;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerActorSystem {

    private final ActorSystem system;
    private TraverserSet results = new TraverserSet();

    public TinkerActorSystem(final Traversal.Admin<?, ?> traversal) {
        this.system = ActorSystem.create("traversal-" + traversal.hashCode());
        this.system.actorOf(Props.create(MasterTraversalActor.class, traversal, new HashPartitioner(traversal.getGraph().get().partitioner(), 3), this.results), "master");
    }

    public Iterator<Traverser.Admin> getResults() {
        return this.results.iterator();
    }

    //////////////

    public static void main(String args[]) throws Exception {
        for (int i = 0; i < 100; i++) {
            final Graph graph = TinkerGraph.open();
            graph.io(GryoIo.build()).readGraph("data/tinkerpop-modern.kryo");
            final GraphTraversalSource g = graph.traversal().withComputer();
            final List<Pair<Integer, Traversal.Admin<?, ?>>> traversals = Arrays.asList(
                    // match() works
                    Pair.with(6, g.V().match(
                            as("a").out("created").as("b"),
                            as("b").in("created").as("c"),
                            as("b").has("name", P.eq("lop"))).where("a", P.neq("c")).select("a", "b", "c").by("name").asAdmin()),
                    // side-effects work
                    Pair.with(1, g.V().repeat(both()).times(2).
                            groupCount("a").by("name").
                            cap("a").asAdmin()),
                    // barriers work and beyond the local star graph works
                    Pair.with(1, g.V().repeat(both()).times(2).hasLabel("person").
                            group().
                            by("name").
                            by(out("created").values("name").dedup().fold()).asAdmin()),
                    // no results works
                    Pair.with(0, g.V().out("blah").asAdmin())
            );
            for (final Pair<Integer,Traversal.Admin<?, ?>> pair : traversals) {
                final Integer count = pair.getValue0();
                final Traversal.Admin<?,?> traversal = pair.getValue1();
                System.out.println("EXECUTING: " + traversal.getBytecode());
                final TinkerActorSystem actors = new TinkerActorSystem(traversal.clone());
                while (!actors.system.isTerminated()) {

                }
                System.out.println(IteratorUtils.asList(actors.getResults()));
                //if(count != 6 && IteratorUtils.count(actors.getResults()) != count)
                //   throw new IllegalStateException();
                System.out.println("//////////////////////////////////\n");
            }
        }
    }


}

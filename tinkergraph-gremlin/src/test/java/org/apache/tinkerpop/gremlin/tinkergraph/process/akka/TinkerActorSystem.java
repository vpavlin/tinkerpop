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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.util.HashPartitioner;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerActorSystem {

    public static enum State {START}

    private final ActorSystem system;

    public TinkerActorSystem(final Traversal.Admin<?, ?> traversal) {
        this.system = ActorSystem.create("traversal-" + traversal.hashCode());
        this.system.actorOf(Props.create(MasterTraversalActor.class, traversal, new HashPartitioner(traversal.getGraph().get().partitioner(), 10)), "master");
    }

    //////////////

    public static void main(String args[]) throws Exception {
        final Graph graph = TinkerGraph.open();
        graph.io(GryoIo.build()).readGraph("data/grateful-dead.kryo");
        /*final Traversal.Admin<?, ?> traversal = graph.traversal().withComputer().V().match(
                as("a").out().as("b"),
                as("b").in().as("c"),
                as("b").has("name", P.eq("lop"))).where("a", P.neq("c")).select("a", "b", "c").by("name").asAdmin();*/
        final Traversal.Admin<?, ?> traversal = graph.traversal().withComputer().V().repeat(both()).times(2).groupCount("a").by("name").cap("a").select(Column.keys).unfold().limit(10).asAdmin();
        new TinkerActorSystem(traversal);
    }


}

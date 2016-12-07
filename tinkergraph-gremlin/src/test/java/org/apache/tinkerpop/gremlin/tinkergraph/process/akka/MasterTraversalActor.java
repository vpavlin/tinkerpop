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

import akka.actor.AbstractActor;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.dispatch.RequiresMessageQueue;
import akka.japi.pf.ReceiveBuilder;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MasterTraversalActor extends AbstractActor implements
        RequiresMessageQueue<TraverserMailbox.TraverserSetSemantics> {

    private Traversal.Admin<?, ?> traversal;
    private final Partitioner partitioner;
    private List<ActorPath> workers;

    public MasterTraversalActor(final Traversal.Admin<?, ?> traversal, final Partitioner partitioner) {
        System.out.println("master[created]: " + self().path());
        traversal.applyStrategies();
        this.traversal = ((TraversalVertexProgramStep) traversal.getStartStep()).computerTraversal.get();
        this.partitioner = partitioner;
        this.initializeWorkers();

        receive(ReceiveBuilder.
                match(Traverser.Admin.class, traverser -> {
                    if (traverser.isHalted())
                        System.out.println("master[result]: " + traverser);
                    else
                        throw new RuntimeException("Master should only process halted traversers: " + traverser);

                }).build()
        );
    }

    public void initializeWorkers() {
        final List<Partition> partitions = this.partitioner.getPartitions();
        this.workers = new ArrayList<>(partitions.size());
        for (final Partition partition : partitions) {
            final ActorRef worker = context().actorOf(Props.
                            create(WorkerTraversalActor.class, this.traversal.clone(), partition, this.partitioner),
                    "worker-" + partition.hashCode());
            this.workers.add(worker.path());
        }
        for (final ActorPath worker : this.workers) {
            context().actorSelection(worker).tell(TinkerActorSystem.State.START, self());
        }
    }
}
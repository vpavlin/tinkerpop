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
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.BarrierMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.BarrierSynchronizationMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.HaltSynchronizationMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.SideEffectMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MasterTraversalActor extends AbstractActor implements RequiresMessageQueue<TraverserMailbox.TraverserSetSemantics> {

    private final Traversal.Admin<?, ?> traversal;
    private final TraversalMatrix<?, ?> matrix;
    private final Partitioner partitioner;
    private List<ActorPath> workers;
    private final Map<String, Set<ActorPath>> synchronizationLocks = new HashMap<>();

    public MasterTraversalActor(final Traversal.Admin<?, ?> traversal, final Partitioner partitioner) {
        System.out.println("master[created]: " + self().path());
        traversal.applyStrategies();
        this.traversal = ((TraversalVertexProgramStep) traversal.getStartStep()).computerTraversal.get();
        this.matrix = new TraversalMatrix<>(this.traversal);
        this.partitioner = partitioner;
        this.initializeWorkers();

        receive(ReceiveBuilder.
                match(Traverser.Admin.class, traverser -> {
                    this.processTraverser(traverser);
                }).
                match(BarrierMessage.class, barrier -> {
                    final Barrier barrierStep = ((Barrier) this.matrix.getStepById(barrier.getStepId()));
                    barrierStep.addBarrier(barrier.getBarrier());
                    broadcast(new BarrierSynchronizationMessage(barrierStep, true));
                }).
                match(BarrierSynchronizationMessage.class, barrierSync -> {
                    Set<ActorPath> locks = this.synchronizationLocks.get(barrierSync.getStepId());
                    if (null == locks) {
                        locks = new HashSet<>();
                        this.synchronizationLocks.put(barrierSync.getStepId(), locks);
                    }
                    locks.add(sender().path());
                    if (locks.size() == this.workers.size()) {
                        final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(barrierSync.getStepId());
                        this.broadcast(new BarrierSynchronizationMessage((Barrier) step, false));
                        while (step.hasNext()) {
                            this.processTraverser(step.next());
                        }
                    }
                }).
                match(SideEffectMessage.class, sideEffect -> {
                    this.traversal.getSideEffects().add(sideEffect.getKey(),sideEffect.getValue());
                    //this.broadcast(new SideEffectMessage(sideEffect.getKey(), sideEffect.getValue()));
                }).
                match(HaltSynchronizationMessage.class, haltSync -> {
                    Set<ActorPath> locks = this.synchronizationLocks.get(Traverser.Admin.HALT);
                    if (null == locks) {
                        locks = new HashSet<>();
                        this.synchronizationLocks.put(Traverser.Admin.HALT, locks);
                    }
                    if (haltSync.isHalt()) {
                        locks.add(sender().path());
                        if (locks.size() == this.workers.size())
                            context().system().terminate();
                    } else {
                        locks.remove(sender().path());
                        this.broadcast(new HaltSynchronizationMessage(true));
                    }
                }).build());
    }

    private void initializeWorkers() {
        final List<Partition> partitions = this.partitioner.getPartitions();
        this.workers = new ArrayList<>(partitions.size());
        for (final Partition partition : partitions) {
            final ActorRef worker = context().actorOf(Props.create(WorkerTraversalActor.class, this.traversal.clone(), partition, this.partitioner), "worker-" + partition.hashCode());
            this.workers.add(worker.path());
        }
        for (final ActorPath worker : this.workers) {
            context().actorSelection(worker).tell(TinkerActorSystem.State.START, self());
        }
    }

    private void broadcast(final Object message) {
        for (final ActorPath worker : this.workers) {
            context().actorSelection(worker).tell(message, self());
        }
    }

    private void processTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted()) {
            System.out.println("master[result]: " + traverser);
            broadcast(new HaltSynchronizationMessage(true));
        } else {
            if (traverser.get() instanceof Element) {
                final Partition otherPartition = this.partitioner.getPartition((Element) traverser.get());
                context().actorSelection("worker-" + otherPartition.hashCode()).tell(traverser, self());
            } else {
                final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
                step.addStart(traverser);
                while (step.hasNext()) {
                    this.processTraverser(step.next());
                }
            }
        }
    }
}
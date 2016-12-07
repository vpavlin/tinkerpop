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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MasterTraversalActor extends AbstractActor implements RequiresMessageQueue<TraverserMailbox.TraverserSetSemantics> {

    private Traversal.Admin<?, ?> traversal;
    private TraversalMatrix<?, ?> matrix;
    private final Partitioner partitioner;
    private List<ActorPath> workers;

    private Map<String, Integer> responseCounter = new HashMap<>();

    public MasterTraversalActor(final Traversal.Admin<?, ?> traversal, final Partitioner partitioner) {
        System.out.println("master[created]: " + self().path());
        traversal.applyStrategies();
        this.traversal = ((TraversalVertexProgramStep) traversal.getStartStep()).computerTraversal.get();
        this.matrix = new TraversalMatrix<>(this.traversal);
        this.partitioner = partitioner;
        this.initializeWorkers();

        /*context().system().scheduler().schedule(
                Duration.create(1, TimeUnit.NANOSECONDS),
                Duration.create(1, TimeUnit.NANOSECONDS),
                () -> self().tell(System.currentTimeMillis(), ActorRef.noSender()),
                context().system().dispatcher());*/

        receive(ReceiveBuilder.
                match(Traverser.Admin.class, traverser -> {
                    this.processTraverser(traverser);
                }).
                match(BarrierMessage.class, barrier -> {
                    final Barrier barrierStep = ((Barrier) this.matrix.getStepById(barrier.getStepId()));
                    barrierStep.addBarrier(barrier.getBarrier());
                    broadcast(new BarrierSynchronizationMessage(barrierStep));
                }).
                match(BarrierSynchronizationMessage.class, barrierSync -> {
                    final Integer counter = this.responseCounter.get(barrierSync.getStepId());
                    final int newCounter = null == counter ? 1 : counter + 1;
                    this.responseCounter.put(barrierSync.getStepId(), newCounter);
                    if (newCounter == this.workers.size()) {
                        final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(barrierSync.getStepId());
                        while (step.hasNext()) {
                            this.processTraverser(step.next());
                        }
                    }
                }).
                match(HaltSynchronizationMessage.class, haltSync -> {
                    final Integer counter = this.responseCounter.get(Traverser.Admin.HALT);
                    final int newCounter = null == counter ? 1 : counter + 1;
                    this.responseCounter.put(Traverser.Admin.HALT, newCounter);
                    if (newCounter == this.workers.size()) {
                        context().system().terminate();
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
            context().actorSelection(worker).tell(message,self());
        }
    }

    private void processTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted()) {
            System.out.println("master[result]: " + traverser);
            broadcast(new HaltSynchronizationMessage());
        } else {
            if (traverser.get() instanceof Element) {
                final Partition otherPartition = this.partitioner.getPartition((Element) traverser.get());
                context().actorSelection("worker-" + otherPartition.hashCode()).tell(traverser,self());
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
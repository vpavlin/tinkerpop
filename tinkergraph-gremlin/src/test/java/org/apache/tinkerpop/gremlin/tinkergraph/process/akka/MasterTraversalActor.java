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
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.dispatch.RequiresMessageQueue;
import akka.japi.pf.ReceiveBuilder;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.BarrierAddMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.BarrierDoneMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.SideEffectAddMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.StartMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.VoteToContinueMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.VoteToHaltMessage;

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
    private final Map<String, ActorSelection> workers = new HashMap<>();
    private final Set<ActorPath> haltSynchronization = new HashSet<>();
    private Barrier barrierLock = null;

    public MasterTraversalActor(final Traversal.Admin<?, ?> traversal, final Partitioner partitioner) {
        System.out.println("master[created]: " + self().path());
        final TraversalStrategies strategies = traversal.getStrategies().clone();
        strategies.removeStrategies(ComputerVerificationStrategy.class, StandardVerificationStrategy.class);
        strategies.addStrategies(ComputerActivationStrategy.instance());
        traversal.setStrategies(strategies);
        traversal.applyStrategies();
        this.traversal = ((TraversalVertexProgramStep) traversal.getStartStep()).computerTraversal.get();
        this.matrix = new TraversalMatrix<>(this.traversal);
        this.partitioner = partitioner;
        this.initializeWorkers();

        receive(ReceiveBuilder.
                match(Traverser.Admin.class, traverser -> {
                    this.processTraverser(traverser);
                }).
                match(BarrierAddMessage.class, barrierMerge -> {
                    final Barrier barrier = (Barrier) this.matrix.getStepById(barrierMerge.getStepId());
                    assert null == this.barrierLock || this.barrierLock == barrier;
                    this.barrierLock = barrier;
                    this.barrierLock.addBarrier(barrierMerge.getBarrier());
                    this.haltSynchronization.remove(sender().path());
                }).
                match(SideEffectAddMessage.class, sideEffect -> {
                    this.traversal.getSideEffects().add(sideEffect.getSideEffectKey(), sideEffect.getSideEffectValue());
                    this.haltSynchronization.remove(sender().path());
                }).
                match(VoteToContinueMessage.class, voteToContinue -> {
                    this.haltSynchronization.remove(sender().path());
                }).
                match(VoteToHaltMessage.class, voteToHalt -> {
                    assert !sender().equals(self());
                    // receive vote to halt messages from worker
                    // when all workers  have voted to halt then terminate the system
                    this.haltSynchronization.add(sender().path());
                    if (this.haltSynchronization.size() == this.workers.size()) {
                        if (null != this.barrierLock) {
                            final Step<?, ?> step = (Step) this.barrierLock;
                            while (step.hasNext()) {
                                this.sendTraverser(step.next());
                            }
                            // broadcast to all workers that the barrier is unlocked
                            this.broadcast(new BarrierDoneMessage(this.barrierLock));
                            this.barrierLock = null;
                            this.haltSynchronization.clear();
                        } else
                            context().system().terminate();
                    }
                }).build());
    }

    private void initializeWorkers() {
        final List<Partition> partitions = this.partitioner.getPartitions();
        for (final Partition partition : partitions) {
            final String workerPathString = "worker-" + partition.hashCode();
            final ActorRef worker = context().actorOf(Props.create(WorkerTraversalActor.class, this.traversal.clone(), partition, this.partitioner), workerPathString);
            this.workers.put(workerPathString, context().actorSelection(worker.path()));
        }
        for (final ActorSelection worker : this.workers.values()) {
            worker.tell(StartMessage.instance(), self());
        }
    }

    private void broadcast(final Object message) {
        for (final ActorSelection worker : this.workers.values()) {
            worker.tell(message, self());
        }
    }

    private void processTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted() || traverser.get() instanceof Element) {
            this.sendTraverser(traverser);
        } else {
            final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
            step.addStart(traverser);
            while (step.hasNext()) {
                this.processTraverser(step.next());
            }
        }
    }

    private void sendTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted()) {
            System.out.println("master[result]: " + traverser);
        } else if (traverser.get() instanceof Element) {
            final Partition partition = this.partitioner.getPartition((Element) traverser.get());
            final ActorRef worker = this.workers.get("worker-" + partition.hashCode()).anchor();
            this.haltSynchronization.remove(worker.path());
            worker.tell(traverser, self());
        } else {
            self().tell(traverser, self());
        }
    }
}
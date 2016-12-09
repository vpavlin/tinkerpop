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
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.dispatch.RequiresMessageQueue;
import akka.japi.pf.ReceiveBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
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
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WorkerTraversalActor extends AbstractActor implements
        RequiresMessageQueue<TraverserMailbox.TraverserSetSemantics> {

    private TraversalMatrix<?, ?> matrix = null;
    private final Partition localPartition;
    private final Partitioner partitioner;
    private boolean voteToHalt = false;
    private final Map<String, ActorSelection> workers = new HashMap<>();

    private Barrier barrierLock = null;

    public WorkerTraversalActor(final Traversal.Admin<?, ?> traversal, final Partition localPartition, final Partitioner partitioner) {
        System.out.println("worker[created]: " + self().path());
        this.matrix = new TraversalMatrix<>(traversal);
        this.matrix.getTraversal().setSideEffects(new DistributedTraversalSideEffects(this.matrix.getTraversal().getSideEffects(), context()));
        this.localPartition = localPartition;
        this.partitioner = partitioner;
        ((GraphStep) traversal.getStartStep()).setIteratorSupplier(localPartition::vertices);

        receive(ReceiveBuilder.
                match(StartMessage.class, start -> {
                    // initial message from master that says: "start processing"
                    final GraphStep step = (GraphStep) this.matrix.getTraversal().getStartStep();
                    while (step.hasNext()) {
                        this.sendTraverser(step.next());
                    }
                    // internal vote to have in mailbox as final message to process
                    self().tell(VoteToHaltMessage.instance(), self());
                }).
                match(Traverser.Admin.class, traverser -> {
                    // internal vote to have in mailbox as final message to process
                    if (this.voteToHalt) {
                        this.voteToHalt = false;
                        master().tell(VoteToContinueMessage.instance(), self());
                    }
                    this.processTraverser(traverser);
                }).
                match(SideEffectAddMessage.class, sideEffect -> {
                    // TODO
                }).
                match(VoteToContinueMessage.class, voteToContinueMessage -> {
                    this.voteToHalt = false;
                    self().tell(VoteToHaltMessage.instance(), self());
                }).
                match(BarrierDoneMessage.class, barrierSync -> {
                    // barrier is complete and processing can continue
                    if (null != this.barrierLock) {
                        this.barrierLock.done();
                        this.barrierLock = null;
                    }
                    // internal vote to have in mailbox as final message to process
                    if (this.voteToHalt) {
                        this.voteToHalt = false;
                        master().tell(VoteToContinueMessage.instance(), self());
                    }
                }).
                match(VoteToHaltMessage.class, haltSync -> {
                    if (sender().equals(master()))
                        this.voteToHalt = false;
                    // if there is a barrier and thus, halting at barrier, then process barrier
                    boolean hasBarrier = null != this.barrierLock && this.barrierLock.hasNextBarrier();
                    if (hasBarrier) {
                        while (this.barrierLock.hasNextBarrier()) {
                            master().tell(new BarrierAddMessage(this.barrierLock), self());
                        }
                    }
                    if (!this.voteToHalt) {
                        // the final message in the worker mail box, tell master you are done processing messages
                        master().tell(VoteToHaltMessage.instance(), self());
                        this.voteToHalt = true;
                    }
                }).build()
        );
    }

    private void processTraverser(final Traverser.Admin traverser) {
        assert !(traverser.get() instanceof Element) || !traverser.isHalted() || this.localPartition.contains((Element) traverser.get());
        final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
        step.addStart(traverser);
        if (step instanceof Barrier) {
            assert null == this.barrierLock || step == this.barrierLock;
            this.barrierLock = (Barrier) step;
        } else {
            while (step.hasNext()) {
                this.sendTraverser(step.next());
            }
        }
    }

    private void sendTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted())
            master().tell(traverser, self());
        else if (traverser.get() instanceof Element && !this.localPartition.contains((Element) traverser.get())) {
            final Partition otherPartition = this.partitioner.getPartition((Element) traverser.get());
            final String workerPathString = "../worker-" + otherPartition.hashCode();
            ActorSelection worker = this.workers.get(workerPathString);
            if (null == worker) {
                worker = context().actorSelection(workerPathString);
                this.workers.put(workerPathString, worker);
            }
            worker.tell(traverser, self());
        } else
            self().tell(traverser, self());
    }

    private ActorRef master() {
        return context().parent();
    }
}

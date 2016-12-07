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
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.BarrierMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.BarrierSynchronizationMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.HaltSynchronizationMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.SideEffectMessage;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WorkerTraversalActor extends AbstractActor implements
        RequiresMessageQueue<TraverserMailbox.TraverserSetSemantics> {

    private final TraversalMatrix<?, ?> matrix;
    private final Partition partition;
    private final Partitioner partitioner;
    private boolean sentHaltMessage = false;


    public WorkerTraversalActor(final Traversal.Admin<?, ?> traversal, final Partition partition, final Partitioner partitioner) {
        System.out.println("worker[created]: " + self().path());
        this.matrix = new TraversalMatrix<>(traversal);

        this.partition = partition;
        this.partitioner = partitioner;
        ((GraphStep) traversal.getStartStep()).setIteratorSupplier(partition::vertices);


        receive(ReceiveBuilder.
                match(TinkerActorSystem.State.class, state -> {
                    this.matrix.getTraversal().setSideEffects(new DistributedTraversalSideEffects(this.matrix.getTraversal().getSideEffects(), context()));
                    final GraphStep step = (GraphStep) this.matrix.getTraversal().getStartStep();
                    while (step.hasNext()) {
                        this.processTraverser(step.next());
                    }
                }).
                match(BarrierSynchronizationMessage.class, barrierSync -> {
                    final Barrier barrier = this.matrix.getStepById(barrierSync.getStepId());
                    if(barrierSync.getLock()) {
                        this.processBarrier(barrier);
                    } else {
                        barrier.done();
                    }
                }).
                match(SideEffectMessage.class, sideEffect -> {
                    // TODO: sideEffect.setSideEffect(this.matrix.getTraversal());
                }).
                match(HaltSynchronizationMessage.class, haltSync -> {
                    sender().tell(new HaltSynchronizationMessage(true), self());
                    this.sentHaltMessage = true;
                }).
                match(Traverser.Admin.class, traverser -> {
                    if (this.sentHaltMessage) {
                        context().parent().tell(new HaltSynchronizationMessage(false), self());
                        this.sentHaltMessage = false;
                    }
                    this.processTraverser(traverser);
                }).build()
        );
    }

    private void processTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted())
            context().parent().tell(traverser, self());
        else if (traverser.get() instanceof Element && !this.partition.contains((Element) traverser.get())) {
            final Partition otherPartition = this.partitioner.getPartition((Element) traverser.get());
            context().actorSelection("../worker-" + otherPartition.hashCode()).tell(traverser, self());
        } else {
            final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
            step.addStart(traverser);
            if (step instanceof Barrier) {
                this.processBarrier((Barrier)step);
            } else {
                while (step.hasNext()) {
                    this.processTraverser(step.next());
                }
            }
        }
    }

    private void processBarrier(final Barrier barrier) {
        while(barrier.hasNextBarrier()) {
            context().parent().tell(new BarrierMessage(barrier), self());
        }
        context().parent().tell(new BarrierSynchronizationMessage(barrier, true), self());
    }
}

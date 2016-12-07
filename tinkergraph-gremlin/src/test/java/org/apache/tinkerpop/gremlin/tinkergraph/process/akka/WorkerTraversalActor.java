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
import akka.japi.pf.ReceiveBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Partition;
import org.apache.tinkerpop.gremlin.structure.Partitioner;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WorkerTraversalActor extends AbstractActor {

    private final TraversalMatrix<?, ?> matrix;
    private final Partition partition;
    private final Partitioner partitioner;

    public WorkerTraversalActor(final Traversal.Admin<?, ?> traversal, final Partition partition, final Partitioner partitioner) {
        System.out.println("worker[created]: " + self().path());
        this.matrix = new TraversalMatrix<>(traversal);
        this.partition = partition;
        this.partitioner = partitioner;
        ((GraphStep) traversal.getStartStep()).setIteratorSupplier(partition::vertices);


        receive(ReceiveBuilder.
                match(Boolean.class, bool -> {
                    final GraphStep step = (GraphStep) this.matrix.getTraversal().getStartStep();
                    while (step.hasNext()) {
                        self().tell(step.next(), self());
                    }
                }).
                match(Traverser.Admin.class, this::processTraverser).build()
        );
    }

    private void processTraverser(final Traverser.Admin traverser) {
        if (traverser.isHalted())
            context().actorSelection("../").tell(traverser, self());
        else {
            final Step<?, ?> step = this.matrix.<Object, Object, Step<Object, Object>>getStepById(traverser.getStepId());
            step.addStart(traverser);
            while (step.hasNext()) {
                final Traverser.Admin<?> end = step.next();
                if (end.get() instanceof Element && !this.partition.contains((Element) end.get())) {
                    final Partition otherPartition = this.partitioner.getPartition((Element) end.get());
                    context().actorSelection("../worker-" + otherPartition.hashCode()).tell(end, self());
                } else {
                    this.processTraverser(end);
                }
            }
        }
    }
}

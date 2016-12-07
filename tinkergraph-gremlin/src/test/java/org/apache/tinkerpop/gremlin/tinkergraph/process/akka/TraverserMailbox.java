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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Envelope;
import akka.dispatch.MailboxType;
import akka.dispatch.MessageQueue;
import akka.dispatch.ProducesMessageQueue;
import com.typesafe.config.Config;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import scala.Option;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserMailbox implements MailboxType, ProducesMessageQueue<TraverserMailbox.TraverserMessageQueue> {

    public static class TraverserMessageQueue implements MessageQueue, TraverserSetSemantics {
        private final AtomicReference<TraverserSet<?>> traverserSet = new AtomicReference<>(new TraverserSet<>());
        private final Queue<Envelope> queue = new ConcurrentLinkedQueue<>();

        // these must be implemented; queue used as example
        public void enqueue(final ActorRef receiver, final Envelope handle) {
            if (handle.message() instanceof Traverser.Admin)
                this.traverserSet.get().add((Traverser.Admin) handle.message());
            else
                queue.offer(handle);
        }

        public Envelope dequeue() {
            return !this.queue.isEmpty() ? this.queue.poll() : new Envelope(this.traverserSet.get().poll(), ActorRef.noSender());
        }

        public int numberOfMessages() {
            return this.queue.size() + this.traverserSet.get().size();
        }

        public boolean hasMessages() {
            return !this.queue.isEmpty() || !this.traverserSet.get().isEmpty();
        }

        public void cleanUp(final ActorRef owner, final MessageQueue deadLetters) {
            for (Envelope handle : this.queue) {
                deadLetters.enqueue(owner, handle);
            }
        }
    }

    // This constructor signature must exist, it will be called by Akka
    public TraverserMailbox(ActorSystem.Settings settings, Config config) {
        // put your initialization code here
    }

    // The create method is called to create the MessageQueue
    public MessageQueue create(Option<ActorRef> owner, Option<ActorSystem> system) {
        return new TraverserMessageQueue();
    }

    public static interface TraverserSetSemantics {

    }
}
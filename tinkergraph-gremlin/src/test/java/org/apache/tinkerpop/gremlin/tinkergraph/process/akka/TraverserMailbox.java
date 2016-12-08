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
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.BarrierDoneMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.VoteToHaltMessage;
import org.apache.tinkerpop.gremlin.tinkergraph.process.akka.messages.SideEffectAddMessage;
import scala.Option;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraverserMailbox implements MailboxType, ProducesMessageQueue<TraverserMailbox.TraverserMessageQueue> {

    public static class TraverserMessageQueue implements MessageQueue, TraverserSetSemantics {
        private final TraverserSet<?> traverserSet = new TraverserSet<>(new ConcurrentHashMap<>());
        private final Queue<Envelope> barrierSyncs = new ConcurrentLinkedQueue<>();
        private final Queue<Envelope> haltSyncs = new ConcurrentLinkedQueue<>();
        private final Queue<Envelope> queue = new ConcurrentLinkedQueue<>();
        private final ActorRef owner;

        public TraverserMessageQueue(final ActorRef owner) {
            this.owner = owner;
        }

        // these must be implemented; queue used as example
        public void enqueue(final ActorRef receiver, final Envelope handle) {
            if (handle.message() instanceof Traverser.Admin)
                this.traverserSet.offer((Traverser.Admin) handle.message());
            else if (handle.message() instanceof SideEffectAddMessage)
                this.queue.offer(handle);
            else if (handle.message() instanceof BarrierDoneMessage)
                this.barrierSyncs.offer(handle);
            else if (handle.message() instanceof VoteToHaltMessage)
                this.haltSyncs.offer(handle);
            else
                this.queue.offer(handle);
        }

        public Envelope dequeue() {
            if (!this.queue.isEmpty())
                return this.queue.poll();
            else if (!this.traverserSet.isEmpty())
                return new Envelope(this.traverserSet.poll(), this.owner);
            else if (!this.barrierSyncs.isEmpty())
                return this.barrierSyncs.poll();
            else
                return this.haltSyncs.poll();
        }

        public int numberOfMessages() {
            return this.queue.size() + this.traverserSet.size() + this.barrierSyncs.size() + this.haltSyncs.size();
        }

        public boolean hasMessages() {
            return !this.queue.isEmpty() || !this.traverserSet.isEmpty() || !this.barrierSyncs.isEmpty() || !this.haltSyncs.isEmpty();
        }

        public void cleanUp(final ActorRef owner, final MessageQueue deadLetters) {
            for (final Envelope handle : this.queue) {
                deadLetters.enqueue(owner, handle);
            }
        }
    }

    // This constructor signature must exist, it will be called by Akka
    public TraverserMailbox(final ActorSystem.Settings settings, final Config config) {
        // put your initialization code here
    }

    // The create method is called to create the MessageQueue
    public MessageQueue create(final Option<ActorRef> owner, final Option<ActorSystem> system) {
        return new TraverserMessageQueue(owner.isEmpty() ? ActorRef.noSender() : owner.get());
    }

    public static interface TraverserSetSemantics {

    }
}
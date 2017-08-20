/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.ipc.netty.channel;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.NettyPipeline;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * A one time-set channel pipeline callback to emit {@link Connection} state for clean
 * disposing. A {@link ContextHandler} is bound to a user-facing {@link MonoSink}
 *
 * @param <CHANNEL> the channel type
 *
 * @author Stephane Maldini
 */
public abstract class ContextHandler<CHANNEL extends Channel>
		implements Disposable, Consumer<Channel> {

	/**
	 * Create a new client context
	 *
	 * @param sink
	 * @param channelOpFactory
	 * @param <CHANNEL>
	 *
	 * @return a new {@link ContextHandler} for clients
	 */
	public static <CHANNEL extends Channel> ContextHandler<CHANNEL> newClientContext(
			MonoSink<Connection> sink,
			ChannelOperations.OnNew<CHANNEL> channelOpFactory) {
		return newClientContext(sink, null, channelOpFactory);
	}

	/**
	 * Create a new client context with optional pool support
	 *
	 * @param sink
	 * @param channelOpFactory
	 * @param pool
	 * @param <CHANNEL>
	 *
	 * @return a new {@link ContextHandler} for clients
	 */
	public static <CHANNEL extends Channel> ContextHandler<CHANNEL> newClientContext(
			MonoSink<Connection> sink,
			ChannelPool pool,
			ChannelOperations.OnNew<CHANNEL> channelOpFactory) {
		if (pool != null) {
			return new PooledClientContextHandler<>(channelOpFactory, sink, pool);
		}
		return new ClientContextHandler<>(channelOpFactory, sink);
	}

	/**
	 * Create a new server context
	 *
	 * @param sink
	 * @param channelOpFactory
	 *
	 * @return a new {@link ContextHandler} for servers
	 */
	public static <CHANNEL extends Channel> ContextHandler<CHANNEL> newServerContext(
			MonoSink<Connection> sink,
			ChannelOperations.OnNew<CHANNEL> channelOpFactory) {
		return new ServerContextHandler<>(channelOpFactory, sink);
	}

	final MonoSink<Connection>             sink;
	final ChannelOperations.OnNew<CHANNEL> channelOpFactory;

	boolean fired;

	/**
	 * @param channelOpFactory
	 * @param sink
	 */
	@SuppressWarnings("unchecked")
	protected ContextHandler(ChannelOperations.OnNew<CHANNEL> channelOpFactory,
			MonoSink<Connection> sink) {
		this.channelOpFactory =
				Objects.requireNonNull(channelOpFactory, "channelOpFactory");
		this.sink = sink;

	}

	/**
	 * Return a new {@link ChannelOperations} or null if the passed message is not
	 * null
	 *
	 * @param channel the current {@link Channel}
	 * @param msg an optional message inbound, meaning the channel has already been
	 * started before
	 *
	 * @return a new {@link ChannelOperations}
	 */
	@SuppressWarnings("unchecked")
	public final ChannelOperations<?, ?> createOperations(Channel channel, Object msg) {

		if (msg != null) {
			ChannelOperations<?, ?> op =
					channelOpFactory.create((CHANNEL) channel, this, msg);

			if (op != null) {
				ChannelOperations old = ChannelOperations.tryGetAndSet(channel, op);

				if (old != null) {
					if (log.isDebugEnabled()) {
						log.debug(channel.toString() + "Mixed pooled connection " + "operations between " + op + " - and a previous one " + old);
					}
					return null;
				}

				channel.pipeline()
				       .get(ChannelOperationsHandler.class).lastContext = this;

				channel.eventLoop()
				       .execute(op::onHandlerStart);
			}
			return op;
		}

		return null;
	}

	/**
	 * Trigger {@link MonoSink#success(Object)} that will signal {@link
	 * reactor.ipc.netty.NettyConnector#newHandler(BiFunction)} returned {@link Mono}
	 * subscriber.
	 *
	 * @param context optional context to succeed the associated {@link MonoSink}
	 */
	public abstract void fireContextActive(Connection context);

	/**
	 * Trigger {@link MonoSink#error(Throwable)} that will signal {@link
	 * reactor.ipc.netty.NettyConnector#newHandler(BiFunction)} returned {@link Mono}
	 * subscriber.
	 *
	 * @param t error to fail the associated {@link MonoSink}
	 */
	public void fireContextError(Throwable t) {
		if (!fired) {
			fired = true;
			sink.error(t);
		}
		else if (AbortedException.isConnectionReset(t)) {
			if (log.isDebugEnabled()) {
				log.error("Connection closed remotely", t);
			}
		}
		else {
			log.error("Error cannot be forwarded to user-facing Mono", t);
		}
	}

	/**
	 * One-time only future setter
	 *
	 * @param future the connect/bind future to associate with and cancel on dispose
	 */
	public abstract void setFuture(Future<?> future);

	/**
	 * @param channel
	 */
	protected void doStarted(Channel channel) {
		//ignore
	}

	@Override
	@SuppressWarnings("unchecked")
	public void accept(Channel channel) {
		try {
			channel.pipeline()
			       .addLast(NettyPipeline.ReactiveBridge,
					       new ChannelOperationsHandler(this));
		}
		catch (Exception t) {
			if (log.isErrorEnabled()) {
				log.error("Error while binding a channelOperation with: " + channel.toString() + " on " + channel.pipeline(),
						t);
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("After pipeline {}",
					channel.pipeline()
					       .toString());
		}
	}

	/**
	 * Cleanly terminate a channel according to the current context handler type. Server
	 * might keep alive and recycle connections, pooled client will release and classic
	 * client will close.
	 *
	 * @param channel the channel to unregister
	 */
	protected void terminateChannel(Channel channel) {
		dispose();
	}

	/**
	 * Return a Publisher to signal onComplete on {@link Channel} close or release.
	 *
	 * @param channel the channel to monitor
	 *
	 * @return a Publisher to signal onComplete on {@link Channel} close or release.
	 */
	protected abstract Publisher<Void> onCloseOrRelease(Channel channel);

	static final Logger log = Loggers.getLogger(ContextHandler.class);
}

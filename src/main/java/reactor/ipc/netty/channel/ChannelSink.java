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
import java.util.function.Consumer;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.MonoSink;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.NettyPipeline;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * A one time-set channel pipeline callback to emit {@link Connection} state for clean
 * disposing. A {@link ChannelSink} is bound to a user-facing {@link MonoSink}
 *
 * @param <SINK> the sink type
 *
 * @author Stephane Maldini
 */
public abstract class ChannelSink<SINK extends Disposable>
		implements Disposable, Consumer<Channel> {

	/**
	 * Create a new client context
	 *
	 * @param sink
	 * @param channelOpFactory
	 *
	 * @return a new {@link ChannelSink} for clients
	 */
	public static ChannelSink<Connection> newClientContext(
			MonoSink<Connection> sink,
			ChannelOperations.OnNew channelOpFactory) {
		return newClientContext(sink, null, channelOpFactory);
	}

	/**
	 * Create a new client context with optional pool support
	 *
	 * @param sink
	 * @param channelOpFactory
	 * @param pool
	 *
	 * @return a new {@link ChannelSink} for clients
	 */
	public static ChannelSink<Connection> newClientContext(
			MonoSink<Connection> sink,
			ChannelPool pool,
			ChannelOperations.OnNew channelOpFactory) {
		if (pool != null) {
			return new PooledClientChannelSink(channelOpFactory, sink, pool);
		}
		return new UnpooledClientChannelSink(channelOpFactory, sink);
	}

	final MonoSink<SINK> sink;
	final ChannelOperations.OnNew        channelOpFactory;

	boolean fired;

	/**
	 * @param channelOpFactory
	 * @param sink
	 */
	@SuppressWarnings("unchecked")
	protected ChannelSink(ChannelOperations.OnNew channelOpFactory,
			MonoSink<SINK> sink) {
		this.channelOpFactory =
				Objects.requireNonNull(channelOpFactory, "channelOpFactory");
		this.sink = sink;

	}

	/**
	 * Return a new {@link ChannelOperations} or null if the passed message is not null
	 *
	 * @param channel the current {@link Channel}
	 * @param msg an optional message inbound, meaning the channel has already been
	 * started before
	 *
	 * @return a new {@link ChannelOperations}
	 */
	@SuppressWarnings("unchecked")
	public final ChannelOperations<?, ?> createOperations(Channel channel, Object msg) {

		ChannelOperations<?, ?> op =
				channelOpFactory.create(channel, this, msg);

		if (op != null) {
			ChannelOperations old = ChannelOperations.tryGetAndSet(channel, op);

			if (old != null) {
				if (log.isDebugEnabled()) {
					log.debug(channel.toString() + "Mixed pooled connection " + "operations between " + op + " - and a previous one " + old);
				}
				return null;
			}

			channel.pipeline()
			       .get(ChannelOperationsHandler.class).lastClientSink = this;

			channel.eventLoop()
			       .execute(op::onHandlerStart);
		}
		return op;
	}

	/**
	 * Trigger {@link MonoSink#success(Object)}
	 *
	 * @param connection optional context to succeed the associated {@link MonoSink}
	 */
	public abstract void fireConnectionActive(SINK connection);

	/**
	 * Trigger {@link MonoSink#error(Throwable)}
	 *
	 * @param t error to fail the associated {@link MonoSink}
	 */
	public void fireConnectionError(Throwable t) {
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
	 * Return a Publisher to signal onComplete on {@link Channel} close or release.
	 *
	 * @param channel the channel to monitor
	 *
	 * @return a Publisher to signal onComplete on {@link Channel} close or release.
	 */
	protected abstract Publisher<Void> onCloseOrRelease(Channel channel);

	static final Logger log = Loggers.getLogger(ChannelSink.class);
}

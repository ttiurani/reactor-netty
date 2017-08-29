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

package reactor.ipc.netty.tcp;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import javax.annotation.Nullable;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.SucceededFuture;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.ConnectionEvents;
import reactor.ipc.netty.FutureMono;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.channel.AbortedException;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.resources.LoopResources;
import reactor.ipc.netty.resources.PoolResources;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class TcpClientAcquire extends TcpClient {

	static final TcpClientAcquire INSTANCE = new TcpClientAcquire(TcpResources.get());

	final PoolResources poolResources;

	TcpClientAcquire(PoolResources poolResources) {
		this.poolResources = Objects.requireNonNull(poolResources, "poolResources");
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<? extends Connection> connect(Bootstrap b) {
		ChannelOperations.OnSetup ops = BootstrapHandlers.channelOperationFactory(b);

		if (b.config()
		     .group() == null) {

			TcpClientRunOn.configure(b,
					LoopResources.DEFAULT_NATIVE,
					TcpResources.get(),
					TcpUtils.findSslContext(b));
		}

		return Mono.create(sink -> {
			ChannelPool pool = poolResources.selectOrCreate(b.config()
			                                                 .remoteAddress(),
					b, b.config()
					 .group());

			DisposableAcquire disposableAcquire = new DisposableAcquire(sink, ops, pool);

			BootstrapHandlers.finalize(b, disposableAcquire);

			disposableAcquire.setFuture(pool.acquire());
		});
	}

	static final class DisposableAcquire implements Connection, ConnectionEvents,
	                                                GenericFutureListener<Future<Channel>> {

		static final Logger log = Loggers.getLogger(DisposableAcquire.class);

		final ChannelPool               pool;
		final DirectProcessor<Void>     onReleaseEmitter;
		final MonoSink<Connection>      sink;
		final ChannelOperations.OnSetup opsFactory;

		Channel channel;
		volatile Future<Channel> future;

		static final AtomicReferenceFieldUpdater<DisposableAcquire, Future> FUTURE =
				AtomicReferenceFieldUpdater.newUpdater(DisposableAcquire.class,
						Future.class,
						"future");

		static final Future DISPOSED = new SucceededFuture<>(null, null);

		DisposableAcquire(MonoSink<Connection> sink, ChannelOperations.OnSetup opsFactory,
				ChannelPool pool) {
			this.opsFactory = Objects.requireNonNull(opsFactory, "opsFactory");
			this.pool = pool;
			this.sink = sink;
			this.onReleaseEmitter = DirectProcessor.create();
		}

		@Override
		public Channel channel() {
			return channel;
		}

		@Override
		public Context currentContext() {
			return sink.currentContext();
		}

		@Override
		@SuppressWarnings("unchecked")
		public void dispose() {
			Future<Channel> f = FUTURE.getAndSet(this, DISPOSED);
			if (f == null || f == DISPOSED) {
				return;
			}
			if (!f.isDone()) {
				return;
			}

			try {
				Channel c = f.get();

				if (!c.eventLoop()
				      .inEventLoop()) {
					c.eventLoop()
					 .execute(() -> disposeOperationThenRelease(c));

				}
				else {
					disposeOperationThenRelease(c);
				}

			}
			catch (Exception e) {
				log.error("Failed releasing channel", e);
				onReleaseEmitter.onError(e);
			}
		}

		@Override
		public void onDispose(Channel channel) {
			log.debug("onConnectionDispose({})", channel);
		}

		@Override
		public void onReceiveError(Channel channel, Throwable error) {
			log.error("onConnectionReceiveError({})", channel);
			sink.error(error);
		}

		@Override
		public void onSetup(Channel channel, @Nullable Object msg) {
			this.channel = channel;
			log.debug("onConnectionSetup({})", channel);
			opsFactory.create(this, this, msg);
		}

		@Override
		public void onStart(Connection connection) {
			log.debug("onConnectionStart({})", connection.channel());
			sink.success(connection);
		}

		@Override
		public void operationComplete(Future<Channel> future) throws Exception {
			if (future.isCancelled()) {
				if (log.isDebugEnabled()) {
					log.debug("Cancelled {}", future.toString());
				}
				return;
			}

			if (DISPOSED == this.future) {
				if (log.isDebugEnabled()) {
					log.debug("Dropping acquisition {} because of {}",
							future,
							"asynchronous user cancellation");
				}
				if (future.isSuccess()) {
					disposeOperationThenRelease(future.get());
				}
				sink.success();
				return;
			}

			if (!future.isSuccess()) {
				if (future.cause() != null) {
					sink.error(future.cause());
				}
				else {
					sink.error(new AbortedException("error while acquiring connection"));
				}
				return;
			}

			Channel c = future.get();

			if (c.eventLoop()
			     .inEventLoop()) {
				connectOrAcquire(c);
			}
			else {
				c.eventLoop()
				 .execute(() -> connectOrAcquire(c));
			}
		}

		@Override
		public Mono<Void> onDispose() {
			return Mono.first(
					Mono.fromDirect(onReleaseEmitter),
					FutureMono.from(channel.closeFuture())
			);
		}

		@SuppressWarnings("unchecked")
		void setFuture(Future<?> future) {
			Objects.requireNonNull(future, "future");

			Future<Channel> f;
			for (; ; ) {
				f = this.future;

				if (f == DISPOSED) {
					if (log.isDebugEnabled()) {
						log.debug("Cancelled existing channel from pool: {}",
								pool.toString());
					}
					sink.success();
					return;
				}

				if (FUTURE.compareAndSet(this, f, future)) {
					break;
				}
			}
			if (log.isDebugEnabled()) {
				log.debug("Acquiring existing channel from pool: {} {}",
						future,
						pool.toString());
			}
			((Future<Channel>) future).addListener(this);

			sink.onCancel(this);
		}

		@SuppressWarnings("unchecked")
		final void connectOrAcquire(Channel c) {
			if (DISPOSED == this.future) {
				if (log.isDebugEnabled()) {
					log.debug("Dropping acquisition {} because of {}",
							"asynchronous user cancellation");
				}
				disposeOperationThenRelease(c);
				sink.success();
				return;
			}

			ChannelHandler op = c.pipeline()
			                     .get(NettyPipeline.ReactiveBridge);

			if (op == null) {
				if (log.isDebugEnabled()) {
					log.debug("Created new pooled channel: " + c.toString());
				}
				c.closeFuture()
				 .addListener(ff -> release(c));
				return;
			}
			if (!c.isActive()) {
				log.debug("Immediately aborted pooled channel, re-acquiring new " + "channel: {}",
						c.toString());
				release(c);
				setFuture(pool.acquire());
				return;
			}
			if (log.isDebugEnabled()) {
				log.debug("Acquired active channel: " + c.toString());
			}
		}


		final void disposeOperationThenRelease(Channel c) {
			ChannelOperations<?, ?> ops = ChannelOperations.get(c);
			//defer to operation dispose if present
			if (ops != null) {
				ops.dispose();
				return;
			}

			release(c);
		}

		final void release(Channel c) {
			if (log.isDebugEnabled()) {
				log.debug("Releasing channel: {}", c.toString());
			}

			pool.release(c)
			    .addListener(f -> {
				    if (!c.isActive()) {
					    return;
				    }
				    if (!Connection.isPersistent(c) && c.isActive()) {
					    c.close();
				    }
				    else if (f.isSuccess()) {
					    onReleaseEmitter.onComplete();
				    }
				    else {
					    onReleaseEmitter.onError(f.cause());
				    }
			    });

		}
	}
}

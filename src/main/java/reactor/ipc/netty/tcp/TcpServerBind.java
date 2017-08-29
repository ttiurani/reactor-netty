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

import java.io.IOException;
import java.util.Objects;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.ConnectionEvents;
import reactor.ipc.netty.DisposableServer;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.resources.LoopResources;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class TcpServerBind extends TcpServer {

	static final TcpServerBind INSTANCE = new TcpServerBind();

	@Override
	public Mono<? extends DisposableServer> bind(ServerBootstrap b) {
		ChannelOperations.OnNew ops = BootstrapHandlers.channelOperationFactory(b);

		if (b.config()
		     .group() == null) {

			TcpServerRunOn.configure(b,
					LoopResources.DEFAULT_NATIVE,
					TcpResources.get(),
					TcpUtils.findSslContext(b));
		}

		return Mono.create(sink -> {
			DisposableTcpServer state = new DisposableTcpServer(sink, ops);

			BootstrapHandlers.finalize(b, state);

			state.setFuture(b.bind());
		});
	}

	static final class DisposableTcpServer
			implements DisposableServer, ChannelFutureListener, ConnectionEvents {

		static final Logger log = Loggers.getLogger(DisposableTcpServer.class);

		final MonoSink<DisposableServer>  sink;
		final ChannelOperations.OnNew     opsFactory;
		final DirectProcessor<Connection> connections;

		ChannelFuture f;

		DisposableTcpServer(MonoSink<DisposableServer> sink,
				ChannelOperations.OnNew opsFactory) {
			this.sink = sink;
			this.opsFactory = opsFactory;
			this.connections = DirectProcessor.create();
		}

		@Override
		public Channel channel() {
			return f.channel();
		}

		@Override
		public Context currentContext() {
			return sink.currentContext();
		}

		@Override
		public Flux<Connection> connections() {
			return connections;
		}

		@Override
		public final void dispose() {
			if (f.channel()
			     .isActive()) {

				f.channel()
				 .close();
			}
			else if (!f.isDone()) {
				f.cancel(true);
			}
		}

		@Override
		public void onConnection(Connection connection) {
			log.debug("onConnection({})", connection);
			connections.onNext(connection);
		}

		@Override
		public void onSetup(Channel channel, Object msg) {
			if (opsFactory.createOnConnected()) {
				opsFactory.create(channel, this, msg)
				          .register();
			}
		}

		@Override
		public void onDispose(Channel channel) {
			if (!channel.isActive()) {
				return;
			}
			if (!Connection.isPersistent(channel)) {
				channel.close();
			}
		}

		@Override
		public void onError(Channel channel, Throwable error) {
			onDispose(channel);
		}

		@Override
		public final void operationComplete(ChannelFuture f) throws Exception {
			if (!f.isSuccess()) {
				if (f.isCancelled()) {
					log.debug("Cancelled Server creation on {}",
							f.channel()
							 .toString());
					return;
				}
				if (f.cause() != null) {
					sink.error(f.cause());
				}
				else {
					sink.error(new IOException("error while binding server to " + f.channel()
					                                                               .toString()));
				}
			}
			else {
				sink.success(this);
			}
		}

		@SuppressWarnings("unchecked")
		final void setFuture(Future<?> future) {
			Objects.requireNonNull(future, "future");

			if (this.f != null) {
				future.cancel(true);
				return;
			}
			if (log.isDebugEnabled()) {
				log.debug("Started server on: {}", future.toString());
			}
			this.f = (ChannelFuture) future;

			if (future.isDone()) {
				try {
					operationComplete((ChannelFuture) future);
				}
				catch (Exception e) {
					sink.error(e);
				}
				return;
			}

			f.addListener(this)
			 .channel()
			 .closeFuture()
			 .addListener(f -> connections.onComplete());

			sink.onCancel(this);
		}
	}
}

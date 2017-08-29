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
import javax.annotation.Nullable;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.ConnectionEvents;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.resources.LoopResources;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class TcpClientConnect extends TcpClient {

	static final TcpClientConnect INSTANCE = new TcpClientConnect();

	@Override
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
			DisposableConnect disposableConnect = new DisposableConnect(sink, ops);

			BootstrapHandlers.finalize(b, disposableConnect);

			disposableConnect.setFuture(b.connect());
		});

	}

	/**
	 * A {@link DisposableConnect} is bound to a user-facing {@link MonoSink}
	 */
	static final class DisposableConnect
			implements Connection, ConnectionEvents, ChannelFutureListener {

		static final Logger log = Loggers.getLogger(DisposableConnect.class);

		final MonoSink<Connection>      sink;
		final ChannelOperations.OnSetup opsFactory;

		ChannelFuture f;
		Channel channel;

		DisposableConnect(MonoSink<Connection> sink,
				ChannelOperations.OnSetup opsFactory) {
			this.opsFactory = Objects.requireNonNull(opsFactory, "opsFactory");
			this.sink = sink;

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
		public final void dispose() {
			if (f == null) {
				return;
			}
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
		public final void operationComplete(ChannelFuture f) throws Exception {
			if (!f.isSuccess()) {
				if (f.isCancelled()) {
					log.debug("Cancelled {}", f.channel());
					return;
				}
				if (f.cause() != null) {
					sink.error(f.cause());
				}
				else {
					sink.error(new IOException("error while connecting to " + f.channel()));
				}
			}
		}

		final void setFuture(Future<?> future) {
			Objects.requireNonNull(future, "future");
			if (this.f != null) {
				future.cancel(true);
				return;
			}
			if (log.isDebugEnabled()) {
				log.debug("Connecting new channel: {}", future.toString());
			}
			this.f = (ChannelFuture) future;

			f.addListener(this);

			sink.onCancel(this);
		}
	}
}

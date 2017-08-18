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
import java.util.function.Supplier;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.util.AttributeKey;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.channel.ContextHandler;
import reactor.ipc.netty.resources.LoopResources;
import reactor.ipc.netty.resources.PoolResources;

/**
 * @author Stephane Maldini
 */
final class TcpClientPooledConnection extends TcpClient {

	static final TcpClientPooledConnection INSTANCE =
			new TcpClientPooledConnection(TcpResources.get());

	static final ChannelOperations.OnNew<Channel> EMPTY = (a, b, c) -> null;

	static final ChannelOperations.OnNew<Channel> CHANNEL_OP_FACTORY =
			(ch, c, msg) -> ChannelOperations.bind(ch, c);

	final PoolResources poolResources;

	TcpClientPooledConnection(PoolResources poolResources) {
		this.poolResources = Objects.requireNonNull(poolResources, "poolResources");
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Mono<? extends Connection> connect(Bootstrap b) {
		BootstrapConfig bc = b.config();

		//Default group and channel
		if (b.config()
		     .group() == null) {

			LoopResources loops = TcpResources.get();

			boolean useNative =
					LoopResources.DEFAULT_NATIVE && !(TcpUtils.findSslContext(b) instanceof JdkSslContext);

			//bypass colocation in default resources
			EventLoopGroup elg = ((Supplier<EventLoopGroup>) loops.onClient(useNative)).get();

			b.group(elg)
			 .channel(loops.onChannel(elg));
		}

		return Mono.create(sink -> {

			ContextHandler<Channel> contextHandler = ContextHandler.newClientContext(sink, EMPTY);
			ChannelPool pool = poolResources.selectOrCreate(bc.remoteAddress(), b, contextHandler, bc.group());

			BootstrapHandlers.configure(b, "init", contextHandler);

			ContextHandler.newClientContext(sink, CHANNEL_OP_FACTORY)
			              .setFuture(pool.acquire());
		});
	}
}

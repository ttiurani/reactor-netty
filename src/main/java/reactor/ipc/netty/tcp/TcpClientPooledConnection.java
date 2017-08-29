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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.handler.ssl.JdkSslContext;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.channel.ChannelSink;
import reactor.ipc.netty.resources.LoopResources;
import reactor.ipc.netty.resources.PoolResources;

/**
 * @author Stephane Maldini
 */
final class TcpClientPooledConnection extends TcpClient {

	static final TcpClientPooledConnection INSTANCE =
			new TcpClientPooledConnection(TcpResources.get());

	static final ChannelOperations.OnNew EMPTY = (a, b, c) -> null;

	final PoolResources poolResources;

	TcpClientPooledConnection(PoolResources poolResources) {
		this.poolResources = Objects.requireNonNull(poolResources, "poolResources");
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<? extends Connection> connect(Bootstrap b) {
		ChannelOperations.OnNew ops = BootstrapHandlers.channelOperationFactory(b);

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

			ChannelSink<Connection> channelSink =
					ChannelSink.newClientContext(sink, EMPTY);
			ChannelPool pool = poolResources.selectOrCreate(bc.remoteAddress(),
					b,
					channelSink,
					bc.group());

			BootstrapHandlers.updateConfiguration(b, "init", channelSink);

			ChannelSink.newClientContext(sink, pool, ops)
			           .setFuture(pool.acquire());
		});
	}
}

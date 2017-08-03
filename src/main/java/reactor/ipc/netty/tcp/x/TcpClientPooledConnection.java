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

package reactor.ipc.netty.tcp.x;

import java.util.Objects;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.AttributeKey;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.resources.PoolResources;
import reactor.ipc.netty.tcp.TcpResources;

/**
 * @author Stephane Maldini
 */
final class TcpClientPooledConnection extends TcpClient {

	static final AttributeKey<Boolean> POOL_KEY = AttributeKey.newInstance("POOL_KEY");

	static final TcpClientPooledConnection INSTANCE =
			new TcpClientPooledConnection(TcpResources.get());

	final PoolResources poolResources;

	TcpClientPooledConnection(PoolResources poolResources) {
		this.poolResources = Objects.requireNonNull(poolResources, "poolResources");
	}

	@Override
	protected Bootstrap configure() {
		return super.configure().attr(POOL_KEY, true);
	}

	@Override
	protected Mono<? extends Connection> connect(Bootstrap b) {
		BootstrapConfig bc = b.config();

		ChannelPool pool = poolResources.selectOrCreate(remote,
				b,
				doHandler(null, sink, secure, remote, null, null),
				options.getLoopResources()
				       .onClient(options.preferNative()));

		contextHandler.setFuture(pool.acquire());
	}
}

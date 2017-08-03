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

import java.net.SocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.BootstrapConfig;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.SocketChannel;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.channel.ContextHandler;

/**
 * @author Stephane Maldini
 */
final class TcpClientSingleConnection extends TcpClient {

	static final TcpClientSingleConnection INSTANCE = new TcpClientSingleConnection();

	@Override
	protected Mono<? extends Connection> connect(Bootstrap b) {
		return Mono.create(sink -> {

			ContextHandler<SocketChannel> contextHandler =
					doHandler(targetHandler, sink, secure, remote, pool, onSetup);

			b.handler(contextHandler);
			contextHandler.setFuture(b.connect());
		});
	}
}

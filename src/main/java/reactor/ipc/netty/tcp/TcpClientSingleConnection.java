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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.JdkSslContext;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.channel.ChannelSink;
import reactor.ipc.netty.resources.LoopResources;

/**
 * @author Stephane Maldini
 */
final class TcpClientSingleConnection extends TcpClient {

	static final TcpClientSingleConnection INSTANCE = new TcpClientSingleConnection();

	@Override
	public Mono<? extends Connection> connect(Bootstrap b) {
		ChannelOperations.OnNew ops = BootstrapHandlers.channelOperationFactory(b);

		if (b.config()
		     .group() == null) {

			LoopResources loops = TcpResources.get();

			boolean useNative =
					LoopResources.DEFAULT_NATIVE && !(TcpUtils.findSslContext(b) instanceof JdkSslContext);

			EventLoopGroup elg = loops.onClient(useNative);

			b.group(elg)
			 .channel(loops.onChannel(elg));
		}

		return Mono.create(sink -> {

			ChannelSink<?> channelSink = ChannelSink.newClientContext(sink, ops);

			BootstrapHandlers.updateConfiguration(b, "init", channelSink);

			channelSink.setFuture(b.connect());
		});
	}
}

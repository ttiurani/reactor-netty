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

package reactor.ipc.netty.udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.channel.ContextHandler;
import reactor.ipc.netty.resources.LoopResources;

/**
 * @author Stephane Maldini
 */
final class UdpClientBind extends UdpClient {

	static final UdpClientBind INSTANCE = new UdpClientBind();

	@Override
	protected Mono<? extends Connection> bind(Bootstrap b) {
		ChannelOperations.OnNew<?> ops = BootstrapHandlers.channelOperationFactory(b);

		//Default group and channel
		if (b.config()
		     .group() == null) {

			EventLoopGroup elg =
					DEFAULT_UDP_LOOPS.onClient(LoopResources.DEFAULT_NATIVE);

			b.group(elg)
			 .channel(DEFAULT_UDP_LOOPS.onDatagramChannel(elg));
		}

		return Mono.create(sink -> {

			ContextHandler<?> contextHandler = ContextHandler.newClientContext(sink, ops);

			BootstrapHandlers.updateConfiguration(b, "init", contextHandler);

			contextHandler.setFuture(b.bind());
		});
	}

	static final int           DEFAULT_UDP_THREAD_COUNT = Integer.parseInt(System.getProperty(
			"reactor.udp.ioThreadCount",
			"" + Schedulers.DEFAULT_POOL_SIZE));
	static final LoopResources DEFAULT_UDP_LOOPS        =
			LoopResources.create("udp", DEFAULT_UDP_THREAD_COUNT, true);
}

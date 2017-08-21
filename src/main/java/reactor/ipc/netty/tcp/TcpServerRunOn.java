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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.JdkSslContext;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.resources.LoopResources;

/**
 * @author Stephane Maldini
 */
final class TcpServerRunOn extends TcpServerOperator {

	final LoopResources loopResources;
	final boolean       preferNative;

	TcpServerRunOn(TcpServer server, LoopResources loopResources, boolean preferNative) {
		super(server);
		this.loopResources = Objects.requireNonNull(loopResources, "loopResources");
		this.preferNative = preferNative;
	}

	@Override
	public ServerBootstrap configure() {
		ServerBootstrap b = source.configure();

		boolean useNative = preferNative && !(sslContext() instanceof JdkSslContext);
		EventLoopGroup selectorGroup = loopResources.onServerSelect(useNative);
		EventLoopGroup elg = loopResources.onServer(useNative);

		return b.group(selectorGroup, elg)
		        .channel(loopResources.onServerChannel(elg));
	}
}

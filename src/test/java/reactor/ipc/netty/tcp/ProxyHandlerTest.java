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
import java.util.function.Consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.tcp.ProxyProvider.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

public class ProxyHandlerTest {

	@Test
	public void addProxyHandler() {
		Bootstrap b = TcpClient.create()
		                       .proxy(ops -> ops.type(Proxy.HTTP)
		                                        .host("proxy")
		                                        .port(8080))
		                       .configure();


		EmbeddedChannel channel = new EmbeddedChannel();

		Consumer<? super Channel> c = BootstrapHandlers.findConfiguration("proxy",
				b.config()
				 .handler());

		Objects.requireNonNull(c, "No proxy configuration!");
		assertThat(channel.pipeline().get(NettyPipeline.ProxyHandler)).isNull();
		c.accept(channel);
		assertThat(channel.pipeline().get(NettyPipeline.ProxyHandler)).isNotNull();
	}
}

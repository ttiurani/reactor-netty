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

import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HandlersTest {

	private ProxyProvider proxyOptions;

	@Before
	public void setUp() {
		this.proxyOptions = ProxyProvider.builder()
		                                 .type(ProxyProvider.Proxy.SOCKS4)
		                                 .host("http://proxy")
		                                 .port(456)
		                                 .build();
	}

	@Test
	public void useProxy() {
		ProxyProvider proxyOptions = this.proxyOptions;
		assertThat(Handlers.useProxy(proxyOptions, "hostName")).isFalse();
		assertThat(Handlers.useProxy(proxyOptions,
				new InetSocketAddress("google.com", 123))).isFalse();

		assertThat(Handlers.useProxy(proxyOptions, "hostName")).isTrue();
		assertThat(Handlers.useProxy(proxyOptions,
				new InetSocketAddress("google.com", 123))).isTrue();

		proxyOptions = ProxyProvider.builder()
		                            .type(ProxyProvider.Proxy.SOCKS4)
		                            .host("http://proxy")
		                            .nonProxyHosts("localhost")
		                            .port(123)
		                            .build();

		assertThat(Handlers.useProxy(proxyOptions, (String) null)).isTrue();
		assertThat(Handlers.useProxy(proxyOptions, (InetSocketAddress) null)).isTrue();
		assertThat(Handlers.useProxy(proxyOptions, "hostName")).isTrue();
		assertThat(Handlers.useProxy(proxyOptions,
				new InetSocketAddress("google.com", 123))).isTrue();
		assertThat(Handlers.useProxy(proxyOptions, "localhost")).isFalse();
		assertThat(Handlers.useProxy(proxyOptions,
				new InetSocketAddress("localhost", 8080))).isFalse();
		assertThat(Handlers.useProxy(proxyOptions,
				new InetSocketAddress("127.0.0.1", 8080))).isFalse();
	}
}
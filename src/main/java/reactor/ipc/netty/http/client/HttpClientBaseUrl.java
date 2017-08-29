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

package reactor.ipc.netty.http.client;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Objects;

import reactor.ipc.netty.tcp.TcpClient;

/**
 * @author Stephane Maldini
 */
final class HttpClientBaseUrl extends HttpClientOperator {

	final String uri;

	HttpClientBaseUrl(HttpClient client, String uri) {
		super(client);
		this.uri = Objects.requireNonNull(uri, "uri");
	}

	@Override
	protected TcpClient tcpConfiguration() {
		TcpClient tcp = source.tcpConfiguration();
	}

	final InetSocketAddress getRemoteAddress(URI uri) {
		Objects.requireNonNull(uri, "uri");
		boolean secure = isSecure(uri);
		int port = uri.getPort() != -1 ? uri.getPort() : (secure ? 443 : 80);
		return useProxy(uri.getHost()) ? InetSocketAddress.createUnresolved(uri.getHost(), port) :
				new InetSocketAddress(uri.getHost(), port);
	}

	static boolean isSecure(URI uri) {
		return uri.getScheme() != null && (uri.getScheme()
		                                      .toLowerCase()
		                                      .equals(HttpClient.HTTPS_SCHEME) || uri.getScheme()
		                                                                             .toLowerCase()
		                                                                             .equals(HttpClient.WSS_SCHEME));
	}
}

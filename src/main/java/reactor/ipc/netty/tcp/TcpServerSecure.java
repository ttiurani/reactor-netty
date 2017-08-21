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

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import reactor.core.Exceptions;

/**
 * @author Stephane Maldini
 */
final class TcpServerSecure extends TcpServerOperator {

	final SslContext sslContext;
	final Duration   handshakeTimeout;

	TcpServerSecure(TcpServer server, SslContext sslContext,
			Duration handshakeTimeout) {
		super(server);
		this.sslContext = Objects.requireNonNull(sslContext, "sslContext");
		this.handshakeTimeout = Objects.requireNonNull(handshakeTimeout, "handshakeTimeout");
	}

	TcpServerSecure(TcpServer server,
			Consumer<? super SslContextBuilder> configurator,
			Duration handshakeTimeout) {
		super(server);
		Objects.requireNonNull(configurator, "configurator");
		this.handshakeTimeout = Objects.requireNonNull(handshakeTimeout, "handshakeTimeout");
		SslContext sslContext;
		try {
			SslContextBuilder builder = SslContextBuilder.forServer(
					DEFAULT_SSL_CONTEXT_SELF.certificate(),
					DEFAULT_SSL_CONTEXT_SELF.privateKey());

			configurator.accept(builder);
			sslContext = builder.build();
		}
		catch (Exception sslException) {
			throw Exceptions.propagate(sslException);
		}

		this.sslContext = sslContext;
	}

	@Override
	public ServerBootstrap configure() {
		return TcpUtils.updateSslSupport(source.configure(), sslContext, handshakeTimeout);
	}

	static final SelfSignedCertificate DEFAULT_SSL_CONTEXT_SELF;
	static final SslContext            DEFAULT_SSL_CONTEXT;

	static {
		SslContext sslContext;
		SelfSignedCertificate cert;
		try {
			cert = new SelfSignedCertificate();
			sslContext =
					SslContextBuilder.forServer(cert.certificate(), cert.privateKey())
					                 .build();
		}
		catch (Exception e) {
			cert = null;
			sslContext = null;
		}
		DEFAULT_SSL_CONTEXT = sslContext;
		DEFAULT_SSL_CONTEXT_SELF = cert;
	}

	@Override
	public SslContext sslContext() {
		return sslContext;
	}
}

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
import io.netty.resolver.DefaultAddressResolverGroup;
import io.netty.resolver.NoopAddressResolverGroup;

/**
 * @author Stephane Maldini
 */
final class TcpClientProxy extends TcpClientOperator {

	final Consumer<? super ProxyProvider.TypeSpec> proxyOptions;

	TcpClientProxy(TcpClient client,
			Consumer<? super ProxyProvider.TypeSpec> proxyOptions) {
		super(client);
		this.proxyOptions = Objects.requireNonNull(proxyOptions, "proxyOptions");
	}

	@Override
	@SuppressWarnings("unchecked")
	public Bootstrap configure() {
		Bootstrap b = source.configure();

		ProxyProvider.Build builder =
				(ProxyProvider.Build) ProxyProvider.builder();

		proxyOptions.accept(builder);

		b = TcpUtils.updateProxySupport(b, builder.build());

		if (b.config()
		     .resolver() == DefaultAddressResolverGroup.INSTANCE) {
			return b.resolver(NoopAddressResolverGroup.INSTANCE);
		}
		return b;
	}
}

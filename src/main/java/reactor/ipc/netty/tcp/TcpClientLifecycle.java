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

import java.util.function.Consumer;

import io.netty.bootstrap.Bootstrap;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.Connection;

/**
 * @author Stephane Maldini
 */
final class TcpClientLifecycle extends TcpClientOperator implements Consumer<Connection> {

	final Consumer<? super Bootstrap>  onConnect;
	final Consumer<? super Connection> onConnected;
	final Consumer<? super Connection> onDisconnect;

	TcpClientLifecycle(TcpClient client,
			Consumer<? super Bootstrap> onConnect,
			Consumer<? super Connection> onConnected,
			Consumer<? super Connection> onDisconnect) {
		super(client);
		this.onConnect = onConnect;
		this.onConnected = onConnected;
		this.onDisconnect = onDisconnect;
	}

	@Override
	public Mono<? extends Connection> connect(Bootstrap b) {
		Mono<? extends Connection> m = source.connect(b);

		if (onConnect != null) {
			m = m.doOnSubscribe(s -> onConnect.accept(b));
		}

		if (onConnected != null) {
			m = m.doOnNext(this);
		}

		if (onDisconnect != null) {
			m = m.doOnNext(c -> c.onDispose(() -> onDisconnect.accept(c)));
		}

		return m;
	}

	@Override
	public void accept(Connection o) {
		onConnected.accept(o);
	}
}

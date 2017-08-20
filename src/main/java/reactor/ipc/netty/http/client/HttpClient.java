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

import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.ByteBufFlux;
import reactor.ipc.netty.ByteBufMono;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.http.HttpResources;
import reactor.ipc.netty.http.websocket.WebsocketInbound;
import reactor.ipc.netty.http.websocket.WebsocketOutbound;
import reactor.ipc.netty.resources.LoopResources;
import reactor.ipc.netty.tcp.ProxyProvider;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;

/**
 * A HttpClient allows to build in a safe immutable way an http client that is
 * materialized and connecting when {@link #connect(Bootstrap)} is ultimately called.
 * <p>
 * <p> Internally, materialization happens in three phases, first {@link #configureTcp()}
 * is called to retrieve a ready to use {@link TcpClient}, then {@link
 * TcpClient#configure()} retrieve a usable {@link Bootstrap} for the final {@link
 * #connect(Bootstrap)} is called.
 * <p> Examples:
 * <pre>
 * {@code
 * HttpClient.create()
 * .uri("http://example.com")
 * .get()
 * .single()
 * .block();
 * }
 * {@code
 * HttpClient.create()
 * .uri("http://example.com")
 * .body(ByteBufFlux.fromByteArray(flux))
 * .post()
 * .single(res -> Mono.just(res.status()))
 * .block();
 * }
 * {@code
 * HttpClient.create()
 * .uri("http://example.com")
 * .body(ByteBufFlux.fromByteArray(flux))
 * .post()
 * .single(res -> Mono.just(res.status()))
 * .block();
 * }
 *
 * @author Stephane Maldini
 */
public abstract class HttpClient {

	public static final String USER_AGENT =
			String.format("ReactorNetty/%s", reactorNettyVersion());

	/**
	 * A ready to consume {@link HttpClient}
	 */
	public interface PreparedHttpClient {

		ByteBufFlux content();

		<V> Flux<V> stream(Function<? super HttpClientResponse, ? extends Publisher<? extends V>> receiver);

		<V> Mono<V> single(Function<? super HttpClientResponse, ? extends Mono<? extends V>> receiver);
	}

	/**
	 * Prepare a pooled {@link HttpClient}
	 *
	 * @return a {@link HttpClient}
	 */
	public static HttpClient create() {
		return HttpClientConnection.INSTANCE;
	}

	/**
	 * Prepare a pooled {@link HttpClient}
	 *
	 * @return a {@link HttpClient}
	 */
	public static HttpClient from(TcpClient tcpClient) {
		return new HttpClientConnection(tcpClient);
	}

	public final HttpClient body(Publisher<?> bodyPublisher) {

	}

	/**
	 * HTTP DELETE to connect the {@link HttpClient}.
	 *
	 * @return a {@link PreparedHttpClient} ready to consume for response
	 */
	public final PreparedHttpClient delete() {
		return request(HttpMethod.DELETE);
	}

	/**
	 * Setup a callback called when {@link Channel} is about to connect.
	 *
	 * @param doOnConnect a runnable observing connected events
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient doOnConnect(Consumer<? super Bootstrap> doOnConnect) {
		Objects.requireNonNull(doOnConnect, "doOnConnect");
		return new HttpClientLifecycle(this, doOnConnect, null, null);
	}

	/**
	 * Setup a callback called after {@link Channel} has been connected.
	 *
	 * @param doOnConnected a consumer observing connected events
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient doOnConnected(Consumer<? super Connection> doOnConnected) {
		Objects.requireNonNull(doOnConnected, "doOnConnected");
		return new HttpClientLifecycle(this, null, doOnConnected, null);
	}

	/**
	 * Setup a callback called after {@link Channel} has been disconnected.
	 *
	 * @param doOnDisconnect a consumer observing disconnected events
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient doOnDisconnect(Consumer<? super Connection> doOnDisconnect) {
		Objects.requireNonNull(doOnDisconnect, "doOnDisconnect");
		return new HttpClientLifecycle(this, null, null, doOnDisconnect);
	}

	/**
	 * Setup all lifecycle callbacks called  on or after {@link Channel} has been
	 * connected and after it has been disconnected.
	 *
	 * @param doOnConnect a consumer observing connect events
	 * @param doOnConnected a consumer observing connected events
	 * @param doOnDisconnect a consumer observing disconnected events
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient doOnLifecycle(Consumer<? super Bootstrap> doOnConnect,
			Consumer<? super Connection> doOnConnected,
			Consumer<? super Connection> doOnDisconnect) {
		Objects.requireNonNull(doOnConnect, "doOnConnected");
		Objects.requireNonNull(doOnConnected, "doOnConnected");
		Objects.requireNonNull(doOnDisconnect, "doOnDisconnect");
		return new HttpClientLifecycle(this, doOnConnect, doOnConnected, doOnDisconnect);
	}

	/**
	 * HTTP GET to connect the {@link HttpClient}.
	 *
	 * @return a {@link PreparedHttpClient} ready to consume for response
	 */
	public final PreparedHttpClient get() {
		return request(HttpMethod.GET);
	}

	/**
	 * Attach an IO handler to react on connected client
	 *
	 * @param handler an IO handler that can dispose underlying connection when {@link
	 * Publisher} terminates.
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient handler(BiFunction<? super HttpClientResponse, ? super HttpClientRequest, ? extends Publisher<Void>> handler) {
		Objects.requireNonNull(handler, "handler");
		return doOnConnected(c -> Mono.fromDirect(handler.apply((HttpClientResponse) c,
				(HttpClientRequest) c))
		                              .subscribe(c.disposeSubscriber()));
	}

	/**
	 * Apply headers configuration.
	 *
	 * @param headerBuilder the  header {@link Consumer} to invoke before sending
	 * websocket handshake
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient headers(Consumer<? super HttpHeaders> headerBuilder) {
		return new HttpClientHeaders(this, headerBuilder);
	}

	/**
	 * Apply an http proxy configuration. Use {@link #tcpConfiguration(Function)} to
	 * access more proxy types including SOCKS4 and SOCKS5.
	 *
	 * @param proxyOptions the http proxy configuration callback
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient httpProxy(Consumer<? super ProxyProvider.AddressSpec> proxyOptions) {
		return tcpConfiguration(tcp -> tcp.proxy(p -> proxyOptions.accept(p.type(
				ProxyProvider.Proxy.HTTP))));
	}

	/**
	 * HTTP OPTIONS to connect the {@link HttpClient}.
	 *
	 * @return a {@link PreparedHttpClient} ready to consume for response
	 */
	public final PreparedHttpClient options() {
		return request(HttpMethod.OPTIONS);
	}

	/**
	 * HTTP PATCH to connect the {@link HttpClient}.
	 *
	 * @return a {@link PreparedHttpClient} ready to consume for response
	 */
	public final PreparedHttpClient patch() {
		return request(HttpMethod.PATCH);
	}

	/**
	 * HTTP POST to connect the {@link HttpClient}.
	 *
	 * @return a {@link PreparedHttpClient} ready to consume for response
	 */
	public final PreparedHttpClient post() {
		return request(HttpMethod.POST);
	}

	/**
	 * HTTP PUT to connect the {@link HttpClient}.
	 *
	 * @return a {@link PreparedHttpClient} ready to consume for response
	 */
	public final PreparedHttpClient put() {
		return request(HttpMethod.PUT);
	}

	/**
	 * Use the passed HTTP method to connect the {@link HttpClient}.
	 *
	 * @param method the HTTP method to send
	 *
	 * @return a {@link PreparedHttpClient} ready to consume for response
	 */
	public PreparedHttpClient request(HttpMethod method) {
		return new MonoHttpClientResponse(this, method);
	}

	/**
	 * Apply {@link Bootstrap} configuration given mapper taking currently configured one
	 * and returning a new one to be ultimately used for socket binding. <p> Configuration
	 * will apply during {@link #configureTcp()} phase.
	 *
	 * @param tcpMapper A tcpClient mapping function to update tcp configuration and
	 * return an enriched tcp client to use.
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient tcpConfiguration(Function<? super TcpClient, ? extends TcpClient> tcpMapper) {
		return new HttpClientTcpConfig(this, tcpMapper);
	}

	/**
	 * Run IO loops on a supplied {@link EventLoopGroup} from the {@link LoopResources}
	 * container. Will prefer native (epoll) implementation if available unless the
	 * environment property {@literal reactor.ipc.netty.epoll} is set to {@literal
	 * false}.
	 * <p>
	 * <p>
	 * <p>
	 * /** Remove any previously applied SSL configuration customization
	 *
	 * @return a new {@link HttpClient}
	 */
	public final HttpClient unproxy() {
		return tcpConfiguration(TcpClient::unproxy);
	}

	/**
	 * @param baseUri
	 *
	 * @return
	 */
	public final HttpClient uri(String baseUri) {
		return new HttpClientUri(this, baseUri);
	}

	/**
	 * Apply a wire logger configuration using {@link TcpServer} category
	 *
	 * @return a new {@link TcpServer}
	 */
	public final HttpClient wiretap() {
		return tcpConfiguration(tcp -> tcp.bootstrap(b -> BootstrapHandlers.updateLogSupport(
				b,
				LOGGING_HANDLER)));
	}

	/**
	 * Apply a wire logger configuration
	 *
	 * @param category the logger category
	 *
	 * @return a new {@link TcpServer}
	 */
	public final HttpClient wiretap(String category) {
		return wiretap(category, LogLevel.DEBUG);
	}

	/**
	 * Apply a wire logger configuration
	 *
	 * @param category the logger category
	 * @param level the logger level
	 *
	 * @return a new {@link TcpServer}
	 */
	public final HttpClient wiretap(String category, LogLevel level) {
		Objects.requireNonNull(category, "category");
		Objects.requireNonNull(level, "level");
		return tcpConfiguration(tcp -> tcp.bootstrap(b -> BootstrapHandlers.updateLogSupport(
				b,
				new LoggingHandler(category, level))));
	}

	/**
	 * WebSocket to connect the {@link HttpClient}.
	 *
	 * @return a {@link PreparedHttpClient} ready to consume for response
	 */
	public final PreparedHttpClient ws() {
		return request(WS, HttpClientRequest::sendWebsocket);
	}

	/**
	 * WebSocket to the passed URL, negotiating one of the passed subprotocols.
	 * <p>
	 * The negotiated subprotocol can be accessed through the {@link HttpClientResponse}
	 * by switching to websocket (using any of the {@link HttpClientResponse#receiveWebsocket()
	 * receiveWebSocket} methods) and using {@link WebsocketInbound#selectedSubprotocol()}.
	 * <p>
	 * To send data through the websocket, use {@link HttpClientResponse#receiveWebsocket(BiFunction)}
	 * and then use the function's {@link WebsocketOutbound}.
	 *
	 * @param subprotocols the subprotocol(s) to negotiate, comma-separated, or null if
	 * not relevant.
	 *
	 * @return a {@link PreparedHttpClient} ready to consume for response
	 */
	public final PreparedHttpClient ws(String subprotocols) {
		return request(WS, req -> req.sendWebsocket(subprotocols));
	}

	/**
	 * Materialize a TcpClient from the parent {@link HttpClient} chain to use with {@link
	 * #connect(Bootstrap)} or separately
	 *
	 * @return a configured {@link TcpClient}
	 */
	protected TcpClient configureTcp() {
		return DEFAULT_TCP_CLIENT;
	}

	/**
	 * Bind the {@link HttpClient} and return a {@link Mono} of {@link Connection}
	 *
	 * @param b the {@link Bootstrap} to bind
	 *
	 * @return a {@link Mono} of {@link Connection}
	 */
	protected abstract Mono<? extends Connection> connect(Bootstrap b);

	static final LoggingHandler LOGGING_HANDLER = new LoggingHandler(HttpClient.class);

	final static HttpMethod     WS              = new HttpMethod("WS");
	final static String         WS_SCHEME       = "ws";
	final static String         WSS_SCHEME      = "wss";
	final static String         HTTP_SCHEME     = "http";
	final static String         HTTPS_SCHEME    = "https";
	static final ChannelOperations.OnNew<?> HTTP_OPS =
			(ch, c, msg) -> HttpClientOperations.bindHttp(ch, c);

	static final Function<Bootstrap, Bootstrap> HTTP_OPS_CONF = b -> {
		BootstrapHandlers.channelOperationFactory(b, HTTP_OPS);
		return b;
	};

	static final TcpClient DEFAULT_TCP_CLIENT = TcpClient.create(HttpResources.get())
	                                                     .bootstrap(HTTP_OPS_CONF)
	                                                     .secure();

	static String reactorNettyVersion() {
		return Optional.ofNullable(HttpClient.class.getPackage()
		                                           .getImplementationVersion())
		               .orElse("dev");
	}
}

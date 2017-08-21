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

package reactor.ipc.netty.http.server;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.channel.ChannelOperations;
import reactor.ipc.netty.channel.ContextHandler;
import reactor.ipc.netty.tcp.TcpServer;

/**
 * A HttpClient allows to build in a safe immutable way an http server that is
 * materialized and connecting when {@link #bind(ServerBootstrap)} is ultimately called.
 * <p>
 * <p> Internally, materialization happens in three phases, first {@link
 * #tcpConfiguration()} is called to retrieve a ready to use {@link TcpServer}, then
 * {@link TcpServer#configure()} retrieve a usable {@link ServerBootstrap} for the final
 * {@link #bind(ServerBootstrap)} is called. <p> Examples:
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
 * .post()
 * .send(Flux.just(bb1, bb2, bb3))
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
public abstract class HttpServer {

	/**
	 * Prepare a pooled {@link HttpServer}
	 *
	 * @return a {@link HttpServer}
	 */
	public static HttpServer create() {
		return HttpServerBind.INSTANCE;
	}

	/**
	 * Prepare a pooled {@link HttpServer}
	 *
	 * @return a {@link HttpServer}
	 */
	public static HttpServer from(TcpServer tcpServer) {
		return new HttpServerBind(tcpServer);
	}

	/**
	 * Bind the {@link HttpServer} and return a {@link Mono} of {@link Connection}. If
	 * {@link Mono} is cancelled, the underlying binding will be aborted. Once the {@link
	 * Connection} has been emitted and is not necessary anymore, disposing main server
	 * loop must be done by the user via {@link Connection#dispose()}.
	 *
	 * If updateConfiguration phase fails, a {@link Mono#error(Throwable)} will be returned;
	 *
	 * @return a {@link Mono} of {@link Connection}
	 */
	public Mono<? extends Connection> bind() {
		ServerBootstrap b;
		try{
			b = tcpConfiguration().configure();
		}
		catch (Throwable t){
			Exceptions.throwIfFatal(t);
			return Mono.error(t);
		}
		return bind(b);
	}

	/**
	 * Enable GZip response compression if the client request presents accept encoding
	 * headers.
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer compress() {
		return tcpConfiguration(COMPRESS_ATTR_CONFIG);
	}

	/**
	 * Enable GZip response compression if the client request presents accept encoding
	 * headers
	 * AND the response reaches a minimum threshold
	 *
	 * @param minResponseSize compression is performed once response size exceeds given
	 * value in byte
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer compress(int minResponseSize) {
		if (minResponseSize < 0) {
			throw new IllegalArgumentException("minResponseSize must be positive");
		}
		return tcpConfiguration(tcp -> tcp.attr(HttpServerOperations.PRODUCE_GZIP, minResponseSize));
	}

	/**
	 * Setup a callback called when {@link io.netty.channel.ServerChannel} is about to
	 * bind.
	 *
	 * @param doOnBind a consumer observing server start event
	 *
	 * @return a new {@link TcpServer}
	 */
	public final HttpServer doOnBind(Consumer<? super ServerBootstrap> doOnBind) {
		return tcpConfiguration(tcp -> tcp.doOnBind(doOnBind));

	}

	/**
	 * Setup a callback called when {@link io.netty.channel.ServerChannel} is about to
	 * bind.
	 *
	 * @param doOnBound a consumer observing server started event
	 *
	 * @return a new {@link TcpServer}
	 */
	public final HttpServer doOnBound(Consumer<? super Connection> doOnBound) {
		return tcpConfiguration(tcp -> tcp.doOnBound(doOnBound));
	}

	/**
	 * Setup a callback called when {@link io.netty.channel.ServerChannel} is about to
	 * bind.
	 *
	 * @param doOnUnbind a consumer observing server stop event
	 *
	 * @return a new {@link TcpServer}
	 */
	public final HttpServer doOnUnbind(Consumer<? super Connection> doOnUnbind) {
		return tcpConfiguration(tcp -> tcp.doOnUnbind(doOnUnbind));
	}

	/**
	 * Attach an IO handler to react on connected server
	 *
	 * @param handler an IO handler that can dispose underlying connection when {@link
	 * Publisher} terminates.
	 *
	 * @return a new {@link HttpServer}
	 */
	@SuppressWarnings("unchecked")
	public final HttpServer handler(BiFunction<? super HttpServerRequest, ? super HttpServerResponse, ? extends Publisher<Void>> handler) {
		Objects.requireNonNull(handler, "handler");
		return doOnBound(c -> Mono.fromDirect(handler.apply((HttpServerRequest) c,
				(HttpServerResponse) c))
		                         .subscribe(c.disposeSubscriber()));
	}

	/**
	 * Disable gzip compression
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer noCompression() {
		return tcpConfiguration(COMPRESS_ATTR_DISABLE);
	}

	/**
	 * Apply {@link ServerBootstrap} configuration given mapper taking currently
	 * configured one and returning a new one to be ultimately used for socket binding.
	 * <p> Configuration will apply during {@link #tcpConfiguration()} phase.
	 *
	 * @param tcpMapper A tcpServer mapping function to update tcp configuration and
	 * return an enriched tcp server to use.
	 *
	 * @return a new {@link HttpServer}
	 */
	public final HttpServer tcpConfiguration(Function<? super TcpServer, ? extends TcpServer> tcpMapper) {
		return new HttpServerTcpConfig(this, tcpMapper);
	}

	/**
	 * Define routes for the server through the provided {@link HttpServerRoutes} builder.
	 *
	 * @param routesBuilder provides a route builder to be mutated in order to define routes.
	 * @return a new {@link HttpServer} starting the router on subscribe
	 */
	public final HttpServer router(Consumer<? super HttpServerRoutes>
			routesBuilder) {
		Objects.requireNonNull(routesBuilder, "routeBuilder");
		HttpServerRoutes routes = HttpServerRoutes.newRoutes();
		routesBuilder.accept(routes);
		return handler(routes);
	}
	/**
	 * Apply a wire logger configuration using {@link TcpServer} category
	 *
	 * @return a new {@link TcpServer}
	 */
	public final HttpServer wiretap() {
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
	public final HttpServer wiretap(String category) {
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
	public final HttpServer wiretap(String category, LogLevel level) {
		Objects.requireNonNull(category, "category");
		Objects.requireNonNull(level, "level");
		return tcpConfiguration(tcp -> tcp.bootstrap(b -> BootstrapHandlers.updateLogSupport(
				b,
				new LoggingHandler(category, level))));
	}

	/**
	 * Bind the {@link HttpServer} and return a {@link Mono} of {@link Connection}
	 *
	 * @param b the {@link ServerBootstrap} to bind
	 *
	 * @return a {@link Mono} of {@link Connection}
	 */
	protected abstract Mono<? extends Connection> bind(ServerBootstrap b);

	/**
	 * Materialize a TcpServer from the parent {@link HttpServer} chain to use with
	 * {@link #bind(ServerBootstrap)} or separately
	 *
	 * @return a configured {@link TcpServer}
	 */
	protected TcpServer tcpConfiguration() {
		return DEFAULT_TCP_SERVER;
	}

	static final LoggingHandler LOGGING_HANDLER = new LoggingHandler(HttpServer.class);

	static final ChannelOperations.OnNew<?> HTTP_OPS = new ChannelOperations.OnNew<Channel>() {
		@Override
		public ChannelOperations<?, ?> create(Channel c,
				ContextHandler<?> contextHandler,
				Object msg) {
			return HttpServerOperations.bindHttp(c, contextHandler, msg);
		}

		@Override
		public boolean createOnChannelActive() {
			return false;
		}
	}

	static final Function<ServerBootstrap, ServerBootstrap> HTTP_OPS_CONF = b -> {
		BootstrapHandlers.channelOperationFactory(b, HTTP_OPS);
		return b;
	};

	static final TcpServer DEFAULT_TCP_SERVER = TcpServer.create()
	                                                     .bindAddress("0.0.0.0")
	                                                     .port(80)
	                                                     .bootstrap(HTTP_OPS_CONF);

	static final Function<TcpServer, TcpServer> COMPRESS_ATTR_CONFIG =
			tcp -> tcp.attr(HttpServerOperations.PRODUCE_GZIP, 0);

	static final Function<TcpServer, TcpServer> COMPRESS_ATTR_DISABLE =
			tcp -> tcp.attr(HttpServerOperations.PRODUCE_GZIP, null);
}

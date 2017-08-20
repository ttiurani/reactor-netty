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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.AttributeKey;
import io.netty.util.NetUtil;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.channel.BootstrapHandlers;
import reactor.ipc.netty.resources.LoopResources;

/**
 * A UdpClient allows to build in a safe immutable way a tcp server that is materialized
 * and connecting when {@link #bind(ServerBootstrap)} is ultimately called.
 * <p>
 * <p> Internally, materialization happens in two phases, first {@link #configure()} is
 * called to retrieve a ready to use {@link ServerBootstrap} then {@link #bind(ServerBootstrap)}
 * is called.
 * <p>
 * <p> Example:
 * <pre>
 * {@code TcpServer.create()
 * .doOnBind(startMetrics)
 * .doOnBound(startedMetrics)
 * .doOnUnbind(stopMetrics)
 * .host("127.0.0.1")
 * .port(1234)
 * .secure()
 * .send(ByteBufFlux.fromByteArrays(pub))
 * .block() }
 *
 * @author Stephane Maldini
 */
public abstract class TcpServer {

	/**
	 * Prepare a {@link TcpServer}
	 *
	 * @return a {@link TcpServer}
	 */
	public static TcpServer create() {
		return TcpServerBind.INSTANCE;
	}

	/**
	 * The address to which this server should bind on subscribe.
	 *
	 * @param bindingAddressSupplier A supplier of the address to bind to.
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer addressSupplier(Supplier<? extends SocketAddress> bindingAddressSupplier) {
		Objects.requireNonNull(bindingAddressSupplier, "bindingAddressSupplier");
		return bootstrap(b -> b.localAddress(bindingAddressSupplier.get()));
	}

	/**
	 * Inject default attribute to the future child {@link Channel} connections. They
	 * will be
	 * available via {@link reactor.ipc.netty.NettyInbound#attr(AttributeKey)}.
	 *
	 * @param key the attribute key
	 * @param value the attribute value
	 * @param <T> the attribute type
	 *
	 * @return a new {@link TcpServer}
	 *
	 * @see Bootstrap#attr(AttributeKey, Object)
	 */
	public final <T> TcpServer attr(AttributeKey<T> key, T value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(value, "value");
		return bootstrap(b -> b.childAttr(key, value));
	}

	/**
	 * Apply {@link Bootstrap} configuration given mapper taking currently configured one
	 * and returning a new one to be ultimately used for socket binding. <p> Configuration
	 * will apply during {@link #configure()} phase.
	 *
	 * @param bootstrapMapper A bootstrap mapping function to update configuration and return an
	 * enriched bootstrap.
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer bootstrap(Function<? super ServerBootstrap, ? extends ServerBootstrap> bootstrapMapper) {
		return new TcpServerBootstrap(this, bootstrapMapper);
	}

	/**
	 * Bind the {@link TcpServer} and return a {@link Mono} of {@link Connection}. If
	 * {@link Mono} is cancelled, the underlying binding will be aborted. Once the {@link
	 * Connection} has been emitted and is not necessary anymore, disposing main server
	 * loop must be done by the user via {@link Connection#dispose()}.
	 *
	 * If updateConfiguration phase fails, a {@link Mono#error(Throwable)} will be returned;
	 *
	 * @return a {@link Mono} of {@link Connection}
	 */
	public final Mono<? extends Connection> bind() {
		ServerBootstrap b;
		try{
			b = configure();
		}
		catch (Throwable t){
			Exceptions.throwIfFatal(t);
			return Mono.error(t);
		}
		return bind(b);
	}

	/**
	 * Setup a callback called when {@link io.netty.channel.ServerChannel} is about to
	 * bind.
	 *
	 * @param doOnBind a consumer observing server start event
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer doOnBind(Consumer<? super ServerBootstrap> doOnBind) {
		Objects.requireNonNull(doOnBind, "doOnBind");
		return new TcpServerLifecycle(this, doOnBind, null, null);

	}

	/**
	 * Setup a callback called when {@link io.netty.channel.ServerChannel} is about to
	 * bind.
	 *
	 * @param doOnBound a consumer observing server started event
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer doOnBound(Consumer<? super Connection> doOnBound) {
		Objects.requireNonNull(doOnBound, "doOnBind");
		return new TcpServerLifecycle(this, null, doOnBound, null);
	}

	/**
	 * Setup a callback called when {@link io.netty.channel.ServerChannel} is about to
	 * bind.
	 *
	 * @param doOnUnbind a consumer observing server stop event
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer doOnUnbind(Consumer<? super Connection> doOnUnbind) {
		Objects.requireNonNull(doOnUnbind, "doOnUnbind");
		return new TcpServerLifecycle(this, null, null, doOnUnbind);
	}

	/**
	 * Setup all lifecycle callbacks called on or after {@link io.netty.channel.ServerChannel}
	 * has been bound and after it has been unbound.
	 *
	 * @param onBind a consumer observing server start event
	 * @param onBound a consumer observing server started event
	 * @param onUnbound a consumer observing server stop event
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer doOnLifecycle(Consumer<? super ServerBootstrap> onBind,
			Consumer<? super Connection> onBound,
			Consumer<? super Connection> onUnbound) {
		Objects.requireNonNull(onBind, "onBind");
		Objects.requireNonNull(onBound, "onBound");
		Objects.requireNonNull(onUnbound, "onUnbound");
		return new TcpServerLifecycle(this, onBind, onBound, onUnbound);
	}

	/**
	 * The host to which this client should connect.
	 *
	 * @param host The host to connect to.
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer host(String host) {
		Objects.requireNonNull(host, "host");
		return bootstrap(b -> b.localAddress(host, getPort(b)));
	}

	/**
	 * Return true if that {@link TcpServer} secured via SSL transport
	 *
	 * @return true if that {@link TcpServer} secured via SSL transport
	 */
	public final boolean isSecure(){
		return sslContext() != null;
	}

	/**
	 * Set a {@link ChannelOption} value for low level connection settings like SO_TIMEOUT
	 * or SO_KEEPALIVE. This will apply to each new channel from remote peer.
	 *
	 * @param key the option key
	 * @param value the option value
	 * @param <T> the option type
	 *
	 * @return new {@link TcpServer}
	 *
	 * @see Bootstrap#option(ChannelOption, Object)
	 */
	public final <T> TcpServer option(ChannelOption<T> key, T value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(value, "value");
		return bootstrap(b -> b.childOption(key, value));
	}

	/**
	 * The port to which this client should connect.
	 *
	 * @param port The port to connect to.
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer port(int port) {
		return bootstrap(b -> b.localAddress(TcpUtils.getHost(b), port));
	}

	/**
	 * Run IO loops on the given {@link EventLoopGroup}.
	 *
	 * @param eventLoopGroup an eventLoopGroup to share
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer runOn(EventLoopGroup eventLoopGroup) {
		Objects.requireNonNull(eventLoopGroup, "eventLoopGroup");
		return runOn(preferNative -> eventLoopGroup);
	}

	/**
	 * Run IO loops on a supplied {@link EventLoopGroup} from the {@link LoopResources}
	 * container. Will prefer native (epoll) implementation if available unless the
	 * environment property {@literal reactor.ipc.netty.epoll} is set to {@literal
	 * false}.
	 *
	 * @param channelResources a {@link LoopResources} accepting native runtime
	 * expectation and returning an eventLoopGroup
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer runOn(LoopResources channelResources) {
		return runOn(channelResources, LoopResources.DEFAULT_NATIVE);
	}

	/**
	 * Run IO loops on a supplied {@link EventLoopGroup} from the {@link LoopResources}
	 * container.
	 *
	 * @param channelResources a {@link LoopResources} accepting native runtime
	 * expectation and returning an eventLoopGroup.
	 * @param preferNative Should the connector prefer native (epoll) if available.
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer runOn(LoopResources channelResources, boolean preferNative) {
		return new TcpServerRunOn(this, channelResources, preferNative);
	}

	/**
	 * Enable default sslContext support. The default {@link SslContext} will be assigned
	 * to with a default value of {@literal 10} seconds handshake timeout unless the
	 * environment property {@literal reactor.ipc.netty.sslHandshakeTimeout} is set.
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer secure() {
		return secure(TcpServerSecure.DEFAULT_SSL_CONTEXT, TcpUtils.DEFAULT_SSL_HANDSHAKE_TIMEOUT);
	}

	/**
	 * Apply an SSL configuration customization via the passed configurator. The builder
	 * will produce the {@link SslContext} to be passed to with a default value of
	 * {@literal 10} seconds handshake timeout unless the environment property {@literal
	 * reactor.ipc.netty.sslHandshakeTimeout} is set.
	 *
	 * @param configurator builder callback for further customization.
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer secure(Consumer<? super SslContextBuilder> configurator) {
		return secure(configurator, TcpUtils.DEFAULT_SSL_HANDSHAKE_TIMEOUT);
	}

	/**
	 * Apply an SSL configuration customization via the passed configurator. The builder
	 * will produce the {@link SslContext} to be passed to with a default value of
	 * {@literal 10} seconds handshake timeout unless the environment property {@literal
	 * reactor.ipc.netty.sslHandshakeTimeout} is set.
	 *
	 * @param configurator builder callback for further customization.
	 * @param handshakeTimeout the handshake timeout duration
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer secure(Consumer<? super SslContextBuilder> configurator,
			Duration handshakeTimeout) {
		return new TcpServerSecure(this, configurator, handshakeTimeout);
	}

	/**
	 * Apply an SSL configuration customization via the passed {@link SslContext}.
	 *
	 * @param sslContext The context to set when configuring SSL
	 * @param handshakeTimeout the handshake timeout duration
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer secure(SslContext sslContext, Duration handshakeTimeout) {
		return new TcpServerSecure(this, sslContext, handshakeTimeout);
	}

	/**
	 * Inject default attribute to the future {@link io.netty.channel.ServerChannel}
	 * selector connection.
	 *
	 * @param key the attribute key
	 * @param value the attribute value
	 * @param <T> the attribute type
	 *
	 * @return a new {@link TcpServer}
	 *
	 * @see Bootstrap#attr(AttributeKey, Object)
	 */
	public final <T> TcpServer selectorAttr(AttributeKey<T> key, T value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(value, "value");
		return bootstrap(b -> b.attr(key, value));
	}

	/**
	 * Set a {@link ChannelOption} value for low level connection settings like SO_TIMEOUT
	 * or SO_KEEPALIVE. This will apply to parent selector channel.
	 *
	 * @param key the option key
	 * @param value the option value
	 * @param <T> the option type
	 *
	 * @return new {@link TcpServer}
	 *
	 * @see Bootstrap#option(ChannelOption, Object)
	 */
	public final <T> TcpServer selectorOption(ChannelOption<T> key, T value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(value, "value");
		return bootstrap(b -> b.option(key, value));
	}

	/**
	 * Return the current {@link SslContext} if that {@link TcpServer} secured via SSL
	 * transport or null
	 *
	 * @return he current {@link SslContext} if that {@link TcpServer} secured via SSL
	 * transport or null
	 */
	public SslContext sslContext(){
		return null;
	}

	/**
	 * Remove any previously applied Proxy configuration customization
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer unsecure() {
		return bootstrap(TcpUtils::removeSslSupport);
	}

	/**
	 * Apply a wire logger configuration using {@link TcpServer} category
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer wiretap() {
		return bootstrap(b -> BootstrapHandlers.updateLogSupport(b, LOGGING_HANDLER));
	}

	/**
	 * Apply a wire logger configuration
	 *
	 * @param category the logger category
	 *
	 * @return a new {@link TcpServer}
	 */
	public final TcpServer wiretap(String category) {
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
	public final TcpServer wiretap(String category, LogLevel level) {
		Objects.requireNonNull(category, "category");
		Objects.requireNonNull(level, "level");
		return bootstrap(b -> BootstrapHandlers.updateLogSupport(b,
				new LoggingHandler(category, level)));
	}

	/**
	 * Materialize a Bootstrap from the parent {@link TcpServer} chain to use with {@link
	 * #bind(ServerBootstrap)} or separately
	 *
	 * @return a configured {@link Bootstrap}
	 */
	protected ServerBootstrap configure() {
		return DEFAULT_BOOTSTRAP.clone();
	}

	/**
	 * Bind the {@link TcpServer} and return a {@link Mono} of {@link Connection}
	 *
	 * @param b the {@link Bootstrap} to bind
	 *
	 * @return a {@link Mono} of {@link Connection}
	 */
	protected abstract Mono<? extends Connection> bind(ServerBootstrap b);

	static final ServerBootstrap DEFAULT_BOOTSTRAP =
			new ServerBootstrap().option(ChannelOption.SO_REUSEADDR, true)
			                     .option(ChannelOption.SO_BACKLOG, 1000)
			                     .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
			                     .childOption(ChannelOption.SO_RCVBUF, 1024 * 1024)
			                     .childOption(ChannelOption.SO_SNDBUF, 1024 * 1024)
			                     .childOption(ChannelOption.AUTO_READ, false)
			                     .childOption(ChannelOption.SO_KEEPALIVE, true)
			                     .childOption(ChannelOption.SO_LINGER, 0)
			                     .childOption(ChannelOption.TCP_NODELAY, true)
			                     .childOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, 30000)
			                     .localAddress(NetUtil.LOCALHOST.getHostName(), TcpUtils.DEFAULT_PORT);

	static {
		BootstrapHandlers.channelOperationFactory(DEFAULT_BOOTSTRAP, TcpUtils.TCP_OPS);
	}

	static final LoggingHandler LOGGING_HANDLER = new LoggingHandler(TcpServer.class);

	@SuppressWarnings("unchecked")
	static int getPort(ServerBootstrap b) {
		if (b.config()
		     .localAddress() instanceof InetSocketAddress) {
			return ((InetSocketAddress) b.config()
			                             .localAddress()).getPort();
		}
		return TcpUtils.DEFAULT_PORT;
	}
}

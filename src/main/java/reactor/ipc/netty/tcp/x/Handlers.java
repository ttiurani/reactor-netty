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
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.resolver.DefaultAddressResolverGroup;
import io.netty.resolver.NoopAddressResolverGroup;
import reactor.core.Exceptions;
import reactor.ipc.netty.NettyPipeline;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 * @author Stephane Maldini
 */
@SuppressWarnings("raw")
final class Handlers {

	static final Logger log = Loggers.getLogger(Handlers.class);

	static void addPipelineConsumer(AbstractBootstrap b,
			String name,
			Consumer<? super Channel> c) {

		ReactorPipelineHandler p;

		ChannelHandler handler = b.config()
		                          .handler();

		if (handler instanceof ReactorPipelineHandler) {
			p = new ReactorPipelineHandler((ReactorPipelineHandler) handler);
		}
		else {
			p = new ReactorPipelineHandler(Collections.emptyList());

			if (handler != null) {
				p.add(new PipelineConsumer(consumer -> consumer.pipeline()
				                                               .addFirst(handler),
						"user"));
			}
		}

		p.add(new PipelineConsumer(c, name));
		b.handler(p);
	}

	static void removePipelineConsumer(AbstractBootstrap b, String name) {
		ChannelHandler handler = b.config()
		                          .handler();

		if (handler instanceof ReactorPipelineHandler) {
			ReactorPipelineHandler rph =
					new ReactorPipelineHandler((ReactorPipelineHandler) handler);

			for (int i = 0; i < rph.size(); i++) {
				if (rph.get(i).name.equals(name)) {
					rph.remove(i);
					break;
				}
			}

			b.handler(rph);
		}

	}

	static Bootstrap addOrUpdateProxySupport(Bootstrap b,
			ProxyProvider proxyOptions) {

		addPipelineConsumer(b, NettyPipeline.ProxyHandler, channel -> {
			if (useProxy(proxyOptions,
					b.config()
					 .remoteAddress())) {

				channel.pipeline()
				       .addFirst(NettyPipeline.ProxyHandler,
						       proxyOptions.newProxyHandler());

				if (log.isDebugEnabled()) {
					channel.pipeline()
					       .addFirst(new LoggingHandler("reactor.ipc.netty.proxy"));
				}
			}
		});

		if (b.config()
		     .resolver() == DefaultAddressResolverGroup.INSTANCE) {
			return b.resolver(NoopAddressResolverGroup.INSTANCE);
		}
		return b;
	}

	static <B extends AbstractBootstrap> B addOrUpdateSslSupport(B b,
			SslContext sslContext,
			Duration sslHandshakeTimeout) {

		addPipelineConsumer(b,
				NettyPipeline.SslHandler,
				new SslSupportConsumer(sslContext, sslHandshakeTimeout, b));

		return b;
	}

	static <B extends AbstractBootstrap> B addOrUpdateLogSupport(B b,
			LoggingHandler handler) {

		addPipelineConsumer(b, NettyPipeline.LoggingHandler, channel -> {
			channel.pipeline()
			       .addLast(NettyPipeline.LoggingHandler, handler);

			if (log.isTraceEnabled() && channel.pipeline()
			                                   .get(NettyPipeline.SslHandler) != null) {
				channel.pipeline()
				       .addBefore(NettyPipeline.SslHandler,
						       NettyPipeline.SslLoggingHandler,
						       new LoggingHandler(SslReadHandler.class));
			}
		});

		return b;
	}

	static SslContext getSslContext(AbstractBootstrap b) {
		ChannelHandler handler = b.config()
		                          .handler();

		if (handler instanceof ReactorPipelineHandler) {
			ReactorPipelineHandler rph = (ReactorPipelineHandler) handler;
			for (int i = 0; i < rph.size(); i++) {
				if (rph.get(i).consumer instanceof SslSupportConsumer) {
					return ((SslSupportConsumer)rph.get(i).consumer).sslContext;
				}
			}
		}
		return null;
	}

	static Bootstrap removeProxySupport(Bootstrap b) {
		removePipelineConsumer(b, NettyPipeline.ProxyHandler);
		return b;
	}

	@SuppressWarnings("unchecked")
	static <B extends AbstractBootstrap> Function<B, B> removeSslSupport() {
		return (Function<B, B>) REMOVE_SECURE;
	}

	static <B extends AbstractBootstrap> B removeSslSupport(B b) {
		removePipelineConsumer(b, NettyPipeline.SslHandler);
		return b;
	}

	static boolean useProxy(ProxyProvider proxyOptions, SocketAddress address) {
		return address instanceof InetSocketAddress && useProxy(proxyOptions,
				((InetSocketAddress) address).getHostName());
	}

	static boolean useProxy(ProxyProvider proxyOptions, String hostName) {
		return proxyOptions.getNonProxyHosts() == null || hostName == null || !proxyOptions.getNonProxyHosts()
		                                                                                   .matcher(
				                                                                                   hostName)
		                                                                                   .matches();
	}

	static final Function<AbstractBootstrap, AbstractBootstrap> REMOVE_SECURE =
			Handlers::removeSslSupport;

	static final Function<Bootstrap, Bootstrap> REMOVE_PROXY =
			Handlers::removeProxySupport;

	static final class SslSupportConsumer implements Consumer<Channel> {

		final SslContext        sslContext;
		final Duration          sslHandshakeTimeout;
		final InetSocketAddress sniInfo;

		SslSupportConsumer(SslContext sslContext,
				Duration sslHandshakeTimeout,
				AbstractBootstrap b) {
			this.sslContext = sslContext;
			this.sslHandshakeTimeout = sslHandshakeTimeout;

			if (b instanceof Bootstrap) {
				SocketAddress sniInfo = ((Bootstrap) b).config()
				                                       .remoteAddress();

				if (sniInfo instanceof InetSocketAddress) {
					this.sniInfo = (InetSocketAddress) sniInfo;
				}
				else {
					this.sniInfo = null;
				}

			}
			else {
				sniInfo = null;
			}
		}

		@Override
		public void accept(Channel channel) {
			SslHandler sslHandler;

			if (sniInfo != null) {
				sslHandler = sslContext.newHandler(channel.alloc(),
						sniInfo.getHostName(),
						sniInfo.getPort());

				if (log.isDebugEnabled()) {
					log.debug("SSL enabled using engine {} and SNI {}",
							sslHandler.engine()
							          .getClass()
							          .getSimpleName(),
							sniInfo);
				}
			}
			else {
				sslHandler = sslContext.newHandler(channel.alloc());

				if (log.isDebugEnabled()) {
					log.debug("SSL enabled using engine {}",
							sslHandler.engine()
							          .getClass()
							          .getSimpleName());
				}
			}

			sslHandler.setHandshakeTimeoutMillis(sslHandshakeTimeout.toMillis());

			if (channel.pipeline()
			           .get(NettyPipeline.ProxyHandler) != null) {
				channel.pipeline()
				       .addAfter(NettyPipeline.ProxyHandler,
						       NettyPipeline.SslHandler,
						       sslHandler);
			}
			else {
				channel.pipeline()
				       .addFirst(NettyPipeline.SslHandler, sslHandler);
			}

			channel.pipeline()
			       .addAfter(NettyPipeline.SslHandler,
					       NettyPipeline.SslReader,
					       new SslReadHandler());
		}
	}

	static final class PipelineConsumer {

		final Consumer<? super Channel> consumer;
		final String                    name;

		PipelineConsumer(Consumer<? super Channel> consumer, String name) {
			this.consumer = consumer;
			this.name = name;
		}
	}

	static final class ReactorPipelineHandler extends ArrayList<PipelineConsumer>
			implements ChannelHandler {

		boolean removed;

		ReactorPipelineHandler(Collection<? extends PipelineConsumer> c) {
			super(c);
		}

		@Override
		public boolean add(PipelineConsumer consumer) {
			for (int i = 0; i < size(); i++) {
				if (get(i).name.equals(consumer.name)) {
					set(i, consumer);
					return true;
				}
			}
			return super.add(consumer);
		}

		@Override
		public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
			if (!removed) {
				removed = true;

				for (int i = 0; i < size(); i++) {
					get(i).consumer.accept(ctx.channel());
				}

				ctx.pipeline()
				   .remove(this);
			}

		}

		@Override
		public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
			removed = true;
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
				throws Exception {
			throw Exceptions.propagate(cause);
		}
	}

	static final class SslReadHandler extends ChannelInboundHandlerAdapter {

		boolean handshakeDone;

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			ctx.read(); //consume handshake
		}

		@Override
		public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
			if (!handshakeDone) {
				ctx.read(); /* continue consuming. */
			}
			super.channelReadComplete(ctx);
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
				throws Exception {
			if (evt instanceof SslHandshakeCompletionEvent) {
				handshakeDone = true;
				if (ctx.pipeline()
				       .context(this) != null) {
					ctx.pipeline()
					   .remove(this);
				}
				SslHandshakeCompletionEvent handshake = (SslHandshakeCompletionEvent) evt;
				if (handshake.isSuccess()) {
					ctx.fireChannelActive();
				}
				else {
					ctx.fireExceptionCaught(handshake.cause());
				}
			}
			super.userEventTriggered(ctx, evt);
		}
	}
}

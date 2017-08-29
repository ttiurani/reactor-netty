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

package reactor.ipc.netty.channel;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedNioFile;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.ipc.netty.ByteBufFlux;
import reactor.ipc.netty.Connection;
import reactor.ipc.netty.ConnectionEvents;
import reactor.ipc.netty.FutureMono;
import reactor.ipc.netty.NettyInbound;
import reactor.ipc.netty.NettyOutbound;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.channel.data.AbstractFileChunkedStrategy;
import reactor.ipc.netty.channel.data.FileChunkedStrategy;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

/**
 * A bridge between an immutable {@link Channel} and {@link NettyInbound} /
 * {@link NettyOutbound}
 *
 * @author Stephane Maldini
 * @since 0.6
 */
public class ChannelOperations<INBOUND extends NettyInbound, OUTBOUND extends NettyOutbound>
		implements NettyInbound, NettyOutbound, Connection, CoreSubscriber<Void> {

	/**
	 * Create a new {@link ChannelOperations} attached to the {@link Channel} attribute
	 * {@link #OPERATIONS_KEY}. Attach the {@link NettyPipeline#ReactiveBridge} handle.
	 *
	 * @param channel the new {@link Channel} connection
	 * @param context the events callback
	 * @param <INBOUND> the {@link NettyInbound} type
	 * @param <OUTBOUND> the {@link NettyOutbound} type
	 *
	 * @return the created {@link ChannelOperations} bridge
	 */
	public static <INBOUND extends NettyInbound, OUTBOUND extends NettyOutbound> ChannelOperations<INBOUND, OUTBOUND> bind(
			Channel channel,
			ConnectionEvents context) {
		@SuppressWarnings("unchecked") ChannelOperations<INBOUND, OUTBOUND> ops =
				new ChannelOperations<>(channel, context);

		return ops;
	}

	/**
	 * Return the current {@link Channel} bound
	 * {@link ChannelOperations} or null if none
	 *
	 * @param ch the current {@link Channel}
	 *
	 * @return the current {@link Channel} bound
	 * {@link ChannelOperations} or null if none
	 */
	public static ChannelOperations<?, ?> get(Channel ch) {
		return ch.attr(OPERATIONS_KEY)
		          .get();
	}


	@Nullable
	static ChannelOperations<?, ?> tryGetAndSet(Channel ch, ChannelOperations<?, ?> ops) {
		Attribute<ChannelOperations> attr = ch.attr(ChannelOperations.OPERATIONS_KEY);
		for (; ; ) {
			ChannelOperations<?, ?> op = attr.get();
			if (op != null) {
				return op;
			}

			if (attr.compareAndSet(null, ops)) {
				return null;
			}
		}
	}

	final    Channel               channel;
	final    FluxReceive           inbound;
	final    DirectProcessor<Void> onInactive;
	final    ConnectionEvents      listener;
	@SuppressWarnings("unchecked")
	volatile Subscription          outboundSubscription;
	protected ChannelOperations(Channel channel,
			ChannelOperations<INBOUND, OUTBOUND> replaced) {
		this(channel, replaced.listener, replaced.onInactive);
	}

	protected ChannelOperations(Channel channel, ConnectionEvents listener) {
		this(channel, listener, DirectProcessor.create());
	}

	protected ChannelOperations(Channel channel,
			ConnectionEvents listener,
			DirectProcessor<Void> processor) {
		this.channel = Objects.requireNonNull(channel, "channel");
		this.listener = listener;
		this.inbound = new FluxReceive(this);
		this.onInactive = processor;
		Mono.fromDirect(listener.onCloseOrRelease(channel))
		    .subscribe(onInactive);
	}

	@Override
	public ByteBufAllocator alloc() {
		return channel.alloc();
	}

	@Override
	public final NettyOutbound sendObject(Publisher<?> dataStream) {
		return then(FutureMono.deferFuture(() -> channel.writeAndFlush(dataStream)));
	}

	@Override
	public final NettyOutbound sendObject(Object msg) {
		return then(FutureMono.deferFuture(() -> channel.writeAndFlush(msg)));
	}

	@Override
	public final Channel channel() {
		return channel;
	}

	@Override
	public ChannelOperations<INBOUND, OUTBOUND> withConnection(Consumer<? super Connection> contextCallback) {
		contextCallback.accept(this);
		return this;
	}

	@Override
	public void dispose() {
		inbound.cancel();
		//TODO shouldn't super.dispose be called there / channel closed?
	}

	@Override
	public CoreSubscriber<Void> disposeSubscriber() {
		return this;
	}

	@Override
	public final boolean isDisposed() {
		return get(channel()) != this;
	}

	@Override
	public final Mono<Void> onDispose() {
		return Mono.fromDirect(onInactive);
	}

	@Override
	public Connection onDispose(final Disposable onDispose) {
		onInactive.subscribe(null, e -> onDispose.dispose(), onDispose::dispose);
		return this;
	}

	@Override
	public final void onComplete() {
		Subscription s =
				OUTBOUND_CLOSE.getAndSet(this, Operators.cancelledSubscription());
		if (s == Operators.cancelledSubscription() || isDisposed()) {
			return;
		}
		onOutboundComplete();
	}

	@Override
	public final void onError(Throwable t) {
		Subscription s =
				OUTBOUND_CLOSE.getAndSet(this, Operators.cancelledSubscription());
		if (s == Operators.cancelledSubscription() || isDisposed()) {
			if(log.isDebugEnabled()){
				log.error("An outbound error could not be processed", t);
			}
			return;
		}
		onOutboundError(t);
	}

	@Override
	public final void onNext(Void aVoid) {
	}

	@Override
	public final void onSubscribe(Subscription s) {
		if (Operators.setOnce(OUTBOUND_CLOSE, this, s)) {
			s.request(Long.MAX_VALUE);
		}
	}

	@Override
	public Flux<?> receiveObject() {
		return inbound;
	}

	@Override
	public final ByteBufFlux receive() {
		return ByteBufFlux.fromInbound(receiveObject(), channel.alloc());
	}

	@Override
	public String toString() {
		return channel.toString();
	}

	/**
	 * Return true if inbound traffic is not expected anymore
	 *
	 * @return true if inbound traffic is not expected anymore
	 */
	protected final boolean isInboundDone() {
		return inbound.inboundDone || !channel.isActive();
	}

	/**
	 * Return true if inbound traffic is not expected anymore
	 *
	 * @return true if inbound traffic is not expected anymore
	 */
	protected final boolean isInboundCancelled() {
		return inbound.isCancelled() || !channel.isActive();
	}


	/**
	 * Return true if inbound traffic is not expected anymore
	 *
	 * @return true if inbound traffic is not expected anymore
	 */
	protected final boolean isOutboundDone() {
		return outboundSubscription == Operators.cancelledSubscription() || !channel.isActive();
	}

	/**
	 * Register the operations into the protected attribute key to be available to
	 * {@link #get(Channel)}.
	 *
	 */
	@SuppressWarnings("unchecked")
	public final void register() {
		channel.attr(OPERATIONS_KEY).set(this);
	}

	/**
	 * React on inbound {@link Channel#read}
	 *
	 * @param ctx the context
	 * @param msg the read payload
	 */
	protected void onInboundNext(ChannelHandlerContext ctx, Object msg) {
		inbound.onInboundNext(msg);
	}

	/**
	 * Replace and complete previous operation inbound
	 *
	 * @param ops a new operations
	 *
	 * @return true if replaced
	 */
	protected final boolean replace(@Nullable ChannelOperations<?, ?> ops) {
		return channel.attr(OPERATIONS_KEY)
		              .compareAndSet(this, ops);
	}

	/**
	 * React on inbound cancel (receive() subscriber cancelled)
	 */
	protected void onInboundCancel() {

	}


	/**
	 * React on inbound completion (last packet)
	 */
	protected void onInboundComplete() {
		if (inbound.onInboundComplete()) {
			listener.onConnection(this);
		}
	}

	/**
	 * React on inbound/outbound completion (last packet)
	 */
	protected void onOutboundComplete() {
		if (log.isDebugEnabled()) {
			log.debug("[{}] {} User Handler requesting close connection", formatName(), channel());
		}
		markPersistent(false);
		onHandlerTerminate();
	}

	/**
	 * React on inbound/outbound error
	 *
	 * @param err the {@link Throwable} cause
	 */
	protected void onOutboundError(Throwable err) {
		discreteRemoteClose(err);
		markPersistent(false);
		onHandlerTerminate();
	}

	/**
	 * Try filtering out remote close unless traced, return true if filtered
	 *
	 * @param err the error to check
	 *
	 * @return true if filtered
	 */
	protected final boolean discreteRemoteClose(Throwable err) {
		if (AbortedException.isConnectionReset(err)) {
			if (log.isDebugEnabled()) {
				log.debug("{} [{}] Connection closed remotely", channel.toString(),
						formatName(),
						err);
			}
			return true;
		}

		log.error("[" + formatName() + "] Error processing connection. Requesting close the channel",
				err);
		return false;
	}

	/**
	 * Final release/close (last packet)
	 */
	protected final void onHandlerTerminate() {
		if (replace(null)) {
			if(log.isTraceEnabled()){
				log.trace("{} Disposing ChannelOperation from a channel", channel(), new Exception
						("ChannelOperation terminal stack"));
			}
			try {
				Operators.terminate(OUTBOUND_CLOSE, this);
				onInactive.onComplete(); //signal senders and other interests
				onInboundComplete(); // signal receiver

			}
			finally {
				channel.pipeline()
				       .fireUserEventTriggered(NettyPipeline.handlerTerminatedEvent());
			}
		}
	}

	/**
	 * React on inbound error
	 *
	 * @param err the {@link Throwable} cause
	 */
	protected final void onInboundError(Throwable err) {
		discreteRemoteClose(err);
		if (inbound.onInboundError(err)) {
			listener.onError(channel, err);
		}
	}

	/**
	 * Return the available parent {@link ChannelSink} for user-facing lifecycle
	 * handling
	 *
	 * @return the available parent {@link ChannelSink}for user-facing lifecycle
	 * handling
	 */
	protected final ConnectionEvents listener() {
		return listener;
	}

	/**
	 * Return formatted name of this operation
	 *
	 * @return formatted name of this operation
	 */
	protected final String formatName() {
		return getClass().getSimpleName()
		                 .replace("Operations", "");
	}

	@Override
	public Context currentContext() {
		return listener.currentContext();
	}

	protected FileChunkedStrategy getFileChunkedStrategy() {
		return ChannelOperations.FILE_CHUNKED_STRATEGY_BUFFER;
	}

	@Override
	public NettyOutbound sendFile(Path file, long position, long count) {
		Objects.requireNonNull(file);
		if (channel.pipeline()
		           .get(SslHandler.class) != null) {
			return sendFileChunked(file);
		}

		return then(Mono.using(() -> FileChannel.open(file, StandardOpenOption.READ),
				fc -> FutureMono.from(channel.writeAndFlush(new DefaultFileRegion(fc,
						position,
						count))),
				fc -> {
					try {
						fc.close();
					}
					catch (IOException ioe) {/*IGNORE*/}
				}));
	}

	@Override
	public final NettyOutbound sendFileChunked(Path file) {
		Objects.requireNonNull(file);
		final FileChunkedStrategy strategy = getFileChunkedStrategy();

		if (channel.pipeline()
		           .get(NettyPipeline.ChunkedWriter) == null) {
			strategy.preparePipeline(this);
		}

		return then(Mono.using(() -> FileChannel.open(file, StandardOpenOption.READ),
				fc -> {
					try {
						return FutureMono.deferFuture(() -> channel.writeAndFlush(
								(strategy.chunkFile(fc))));
					}
					catch (Exception e) {
						return Mono.error(e);
					}
				},
				fc -> {
					try {
						fc.close();
					}
					catch (IOException ioe) {/*IGNORE*/}
					finally {
						strategy.cleanupPipeline(this);
					}
				}));
	}

	/**
	 * A {@link ChannelOperations} factory
	 */
	@FunctionalInterface
	public interface OnNew {

		/**
		 * Create a new {@link ChannelOperations} given a netty channel, a parent
		 * {@link ChannelSink} and an optional message (nullable).
		 *
		 * @param c a {@link Channel}
		 * @param listener a {@link ConnectionEvents}
		 * @param msg an optional message
		 *
		 * @return a new {@link ChannelOperations}
		 */
		ChannelOperations<?, ?> create(Channel c, ConnectionEvents listener, Object msg);

		/**
		 * True if {@link ChannelOperations} should be created by
		 * {@link ChannelOperationsHandler} on channelActive event
		 *
		 * @return true if {@link ChannelOperations} should be created by
		 * {@link ChannelOperationsHandler} on channelActive event
		 */
		default boolean createOnConnected(){
			return true;
		}
	}
	/**
	 * The attribute in {@link Channel} to store the current {@link ChannelOperations}
	 */
	protected static final AttributeKey<ChannelOperations> OPERATIONS_KEY = AttributeKey.newInstance("nettyOperations");

	static final Logger     log  = Loggers.getLogger(ChannelOperations.class);

	static final AtomicReferenceFieldUpdater<ChannelOperations, Subscription>
			OUTBOUND_CLOSE = AtomicReferenceFieldUpdater.newUpdater(ChannelOperations.class,
			Subscription.class,
			"outboundSubscription");

	static final FileChunkedStrategy<ByteBuf> FILE_CHUNKED_STRATEGY_BUFFER =
			new AbstractFileChunkedStrategy<ByteBuf>() {

				@Override
				public ChunkedInput<ByteBuf> chunkFile(FileChannel fileChannel) {
					try {
						//TODO tune the chunk size
						return new ChunkedNioFile(fileChannel, 1024);
					}
					catch (IOException e) {
						throw Exceptions.propagate(e);
					}
				}
			};
}
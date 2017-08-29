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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPool;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.SucceededFuture;
import org.reactivestreams.Publisher;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.MonoSink;
import reactor.ipc.netty.Connection;
import reactor.util.Logger;
import reactor.util.Loggers;

/**
 *
 * @author Stephane Maldini
 */
final class PooledClientChannelSink
		extends ChannelSink<Connection>
		implements GenericFutureListener<Future<Channel>> {

	static final Logger log = Loggers.getLogger(PooledClientChannelSink.class);

	final ChannelPool           pool;
	final DirectProcessor<Void> onReleaseEmitter;

	volatile Future<Channel> future;

	static final AtomicReferenceFieldUpdater<PooledClientChannelSink, Future> FUTURE =
			AtomicReferenceFieldUpdater.newUpdater(PooledClientChannelSink.class,
					Future.class,
					"future");

	static final Future DISPOSED = new SucceededFuture<>(null, null);

	PooledClientChannelSink(ChannelOperations.OnNew channelOpFactory,
			MonoSink<Connection> sink,
			ChannelPool pool) {
		super(channelOpFactory, sink);
		this.pool = pool;
		this.onReleaseEmitter = DirectProcessor.create();
	}

	@Override
	public void fireConnectionActive(Connection context) {
		if (!fired) {
			fired = true;
			if(context != null) {
				sink.success(context);
			}
			else{
				sink.success();
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setFuture(Future<?> future) {
		Objects.requireNonNull(future, "future");

		Future<Channel> f;
		for (; ; ) {
			f = this.future;

			if (f == DISPOSED) {
				if (log.isDebugEnabled()) {
					log.debug("Cancelled existing channel from pool: {}", pool.toString());
				}
				sink.success();
				return;
			}

			if (FUTURE.compareAndSet(this, f, future)) {
				break;
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Acquiring existing channel from pool: {} {}", future, pool
					.toString());
		}
		((Future<Channel>) future).addListener(this);
	}

	@Override
	public void operationComplete(Future<Channel> future) throws Exception {
		if (future.isCancelled()) {
			if (log.isDebugEnabled()) {
				log.debug("Cancelled {}", future.toString());
			}
			return;
		}

		if (DISPOSED == this.future) {
			if (log.isDebugEnabled()) {
				log.debug("Dropping acquisition {} because of {}",
						future, "asynchronous user cancellation");
			}
			if (future.isSuccess()) {
				disposeOperationThenRelease(future.get());
			}
			sink.success();
			return;
		}

		if (!future.isSuccess()) {
			if (future.cause() != null) {
				fireConnectionError(future.cause());
			}
			else {
				fireConnectionError(new AbortedException("error while acquiring connection"));
			}
			return;
		}


		Channel c = future.get();

		if (c.eventLoop()
		     .inEventLoop()) {
			connectOrAcquire(c);
		}
		else {
			c.eventLoop()
			 .execute(() -> connectOrAcquire(c));
		}
	}

	@Override
	protected Publisher<Void> onCloseOrRelease(Channel channel) {
		return onReleaseEmitter;
	}

	@SuppressWarnings("unchecked")
	final void connectOrAcquire(Channel c) {
		if (DISPOSED == this.future) {
			if (log.isDebugEnabled()) {
				log.debug("Dropping acquisition {} because of {}",
						"asynchronous user cancellation");
			}
			disposeOperationThenRelease(c);
			sink.success();
			return;
		}

		ChannelOperationsHandler op = c.pipeline()
		                               .get(ChannelOperationsHandler.class);

		if (op == null) {
			if (log.isDebugEnabled()) {
				log.debug("Created new pooled channel: " + c.toString());
			}
			c.closeFuture().addListener(ff -> release(c));
			return;
		}
		if (!c.isActive()) {
			log.debug("Immediately aborted pooled channel, re-acquiring new " + "channel: {}", c.toString());
			release(c);
			setFuture(pool.acquire());
			return;
		}
		if (log.isDebugEnabled()) {
			log.debug("Acquired active channel: " + c.toString());
		}
		if(createOperations(c, null) == null){
			setFuture(pool.acquire());
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void dispose() {
		Future<Channel> f = FUTURE.getAndSet(this, DISPOSED);
		if (f == null || f == DISPOSED) {
			return;
		}
		if (!f.isDone()) {
			return;
		}

		try {
			Channel c = f.get();

			if (!c.eventLoop()
			      .inEventLoop()) {
				c.eventLoop()
				 .execute(() -> disposeOperationThenRelease(c));

			}
			else {
				disposeOperationThenRelease(c);
			}

		}
		catch (Exception e) {
			log.error("Failed releasing channel", e);
			onReleaseEmitter.onError(e);
		}
	}

	final void disposeOperationThenRelease(Channel c){
		ChannelOperations<?, ?> ops = ChannelOperations.get(c);
		//defer to operation dispose if present
		if (ops != null) {
			ops.dispose();
			return;
		}

		release(c);
	}

	final void release(Channel c) {
		if (log.isDebugEnabled()) {
			log.debug("Releasing channel: {}", c.toString());
		}

		pool.release(c)
		    .addListener(f -> {
			    if (!c.isActive()) {
				    return;
			    }
			    if (!Connection.isPersistent(c) && c.isActive()) {
				    c.close();
			    }
			    else if (f.isSuccess()) {
				    onReleaseEmitter.onComplete();
			    }
			    else {
				    onReleaseEmitter.onError(f.cause());
			    }
		    });

	}
}

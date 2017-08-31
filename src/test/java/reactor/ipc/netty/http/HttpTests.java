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
package reactor.ipc.netty.http;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.DisposableServer;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.client.HttpClientException;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;
import reactor.test.StepVerifier;

/**
 * @author Violeta Georgieva
 */
public class HttpTests {

	@Test
	public void httpRespondsEmpty() {
		DisposableServer server =
				HttpServer.from(TcpServer.create().port(0))
				          .router(r ->
				              r.post("/test/{param}", (req, res) -> Mono.empty()))
				          .bindNow(Duration.ofSeconds(30));

		HttpClient client =
				HttpClient.from(TcpClient.create()
				                         .host("localhost")
				                         .port(server.address().getPort()));

		Mono<ByteBuf> content = client.headers(b -> b.add("Content-Type", "text/plain"))
				            .post()
				            .uri("/test/World")
				            .send((req, out) -> out.sendString(Mono.just("Hello")
				                                                   .log("client-send")))
				            .responseContent()
				            .log("client-received")
				            .next()
				            .doOnError(t -> System.err.println("Failed requesting server: " + t.getMessage()));

		StepVerifier.create(content)
				    .expectComplete()
				    .verify(Duration.ofSeconds(5000));

		server.dispose();
	}

	@Test
	public void httpRespondsToRequestsFromClients() {
		DisposableServer server =
				HttpServer.from(TcpServer.create().port(0))
				          .router(r ->
				              r.post("/test/{param}", (req, res) ->
				                  res.sendString(req.receive()
				                                    .asString()
				                                    .log("server-received")
				                                    .map(it -> it + ' ' + req.param("param") + '!')
				                                    .log("server-reply"))))
				          .bindNow(Duration.ofSeconds(30));

		HttpClient client =
				HttpClient.from(TcpClient.create()
						                 .host("localhost")
						                 .port(server.address().getPort()));

		Mono<String> content =
				client.headers(b -> b.add("Content-Type", "text/plain"))
				      .post()
				      .uri("/test/World")
				      .send((req, out) -> out.sendString(Flux.just("Hello")
				                                             .log("client-send")))
				      .responseContent()
				      .aggregate()
				      .asString()
				      .log("client-received")
				      .doOnError(t -> System.err.println("Failed requesting server: " + t.getMessage()));

		StepVerifier.create(content)
				    .expectNextMatches(s -> s.equals("Hello World!"))
				    .expectComplete()
				    .verify(Duration.ofSeconds(5000));

		server.dispose();
	}

	@Test
	public void httpErrorWithRequestsFromClients() throws Exception {
		CountDownLatch errored = new CountDownLatch(1);

		DisposableServer server =
				HttpServer.from(TcpServer.create().port(0))
						  .router(r -> r.get("/test", (req, res) -> {throw new RuntimeException();})
						                .get("/test2", (req, res) -> res.send(Flux.error(new Exception()))
						                                               .then()
						                                               .log("send")
						                                               .doOnError(t -> errored.countDown()))
						                .get("/test3", (req, res) -> Flux.error(new Exception())))
						  .bindNow(Duration.ofSeconds(30));

		HttpClient client =
				HttpClient.from(TcpClient.create()
						                 .host("localhost")
						                 .port(server.address().getPort()));

		Mono<Integer> code =
				client.get()
				      .uri("/test")
				      .response()
				      .flatMap(res -> Mono.just(res.status().code()))
				      .log("received-status-1");

		StepVerifier.create(code)
				    .expectError(HttpClientException.class)
				    .verify(Duration.ofSeconds(30));

		ByteBuf content =
				client.get()
				      .uri("/test2")
				      .responseContent()
				      .log("received-status-2")
				      .next()
				      .block(Duration.ofSeconds(30));

		Assertions.assertThat(errored.await(30, TimeUnit.SECONDS)).isTrue();
		Assertions.assertThat(content).isNull();

		code = client.get()
				     .uri("/test3")
				     .response()
				     .flatMapMany(res -> Flux.just(res.status().code()))
				     .log("received-status-3")
				     .next();

		StepVerifier.create(code)
				    .expectError(HttpClientException.class)
				    .verify(Duration.ofSeconds(30));

		server.dispose();
	}

	@Test
	public void webSocketRespondsToRequestsFromClients() {
		AtomicInteger clientRes = new AtomicInteger();
		AtomicInteger serverRes = new AtomicInteger();

		DisposableServer server =
				HttpServer.from(TcpServer.create().port(0))
				          .router(r -> r.get("/test/{param}", (req, res) -> {
				              System.out.println(req.requestHeaders().get("test"));
				              return res.addHeader("content-type", "text/plain")
				                        .sendWebsocket((in, out) ->
				                            out.options(c -> c.flushOnEach())
				                               .sendString(in.receive()
				                                             .asString()
				                                             .publishOn(Schedulers.single())
				                                             .doOnNext(s -> serverRes.incrementAndGet())
				                                             .map(it -> it + ' ' + req.param("param") + '!')
				                                             .log("server-reply")));
				          }))
				          .bindNow(Duration.ofSeconds(5));

		HttpClient client = HttpClient.from(TcpClient.create()
				                                     .host("localhost")
				                                     .port(server.address().getPort()));

				client.request(HttpMethod.GET)
				      .uri("/test/World")
				      .send((req, out) -> {
				          req.header("Content-Type", "text/plain")
				             .header("test", "test");
				          return out.options(c -> c.flushOnEach()); //TODO
				      });
		/*

	//prepare an http websocket request-reply flow
	def content = client.get('/test/World') { req ->
	  //prepare content-type
	  req.header('Content-Type', 'text/plain').header("test", "test")

	  //return a producing stream to send some data along the request
	  req.options { o -> o.flushOnEach() }
			  .sendWebsocket()
			  .sendString(Flux
				.range(1, 1000)
				.log('client-send')
				.map { it.toString() })
	}.flatMapMany {
	  replies
		->
		//successful handshake, listen for the first returned next replies and pass it downstream
		replies
				.receive()
				.asString()
				.log('client-received')
				.publishOn(Schedulers.parallel())
				.doOnNext { clientRes.incrementAndGet() }
	}
	.take(1000)
			.collectList()
			.cache()
			.doOnError {
	  //something failed during the request or the reply processing
	  println "Failed requesting server: $it"
	}


	println "STARTING: server[$serverRes] / client[$clientRes]"

	then: "data was recieved"
	//the produced reply should be there soon
	//content.block(Duration.ofSeconds(15))[1000 - 1] == "1000 World!"
	content.block(Duration.ofSeconds(30))[1000 - 1] == "1000 World!"

	cleanup: "the client/server where stopped"
	println "FINISHED: server[$serverRes] / client[$clientRes]"
	//note how we order first the client then the server shutdown
	server?.dispose()
		 */

		server.dispose();
	}
}

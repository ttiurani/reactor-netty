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
import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.netty.handler.codec.http.HttpMethod;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.DisposableServer;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.server.HttpServer;
import reactor.ipc.netty.tcp.TcpClient;
import reactor.ipc.netty.tcp.TcpServer;
import reactor.test.StepVerifier;

/**
 * @author Violeta Georgieva
 */
public class HttpResponseStatusCodesHandlingTests {

	@Test
	public void httpStatusCode404IsHandledByTheClient() {
		DisposableServer server =
				HttpServer.from(TcpServer.create().port(0))
				          .router(r -> r.post("/test", (req, res) -> res.send(req.receive()
				                                                                 .log("server-received"))))
				          .bindNow(Duration.ofSeconds(30));

		HttpClient client = HttpClient.from(TcpClient.create().host("localhost").port(server.address().getPort()));

		List<String> replyReceived = new ArrayList<>();
		Mono<String> content = client.request(HttpMethod.GET)
				                     .uri("/unsupportedURI")
				                     .send((req, out) -> {
				                         req.addHeader("Content-Type", "text/plain");
				                         return out.sendString(Flux.just("Hello")
				                                                   .log("client-send"));
				                     })
				                     .responseContent()
				                     .asString()
				                     .log("client-received")
				                     .doOnNext(s -> replyReceived.add(s))
				                     .next()
				                     .doOnError(t -> System.err.println("Failed requesting server: " + t.getMessage()));

		StepVerifier.create(content)
				    .expectErrorMatches(t -> t.getMessage().equals("HTTP request failed with code: 404.\nFailing URI: " +
						"/unsupportedURI"))
				    .verify(Duration.ofSeconds(30));

		Assertions.assertThat(replyReceived).isEmpty();

		server.dispose();
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Properties;
import java.util.concurrent.CompletionStage;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.enhanced.SequenceStyleGenerator;
import org.hibernate.reactive.id.impl.ReactiveGeneratorWrapper;
import org.hibernate.reactive.id.impl.ReactiveIdentifierGeneratorFactory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.type.LongType;

import org.junit.Test;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;

public class BlockingIdentifierGeneratorTest extends BaseReactiveTest {

	protected static boolean RUN_FULL_TESTS = Boolean.getBoolean( "ogm.runFullStressTests" );
	protected static int PARALLEL_THREADS = Runtime.getRuntime().availableProcessors();
	protected static int NUMBER_OF_TASKS = PARALLEL_THREADS * 3;
	protected static int INCREASES_PER_TASK = RUN_FULL_TESTS ? 100_000 : 10;

//
//	protected IncrementJob[] runJobs(final NextValueRequest nextValueRequest) throws InterruptedException {
//		ExecutorService executorService = Executors.newWorkStealingPool( PARALLEL_THREADS );
//		IncrementJob[] runJobs = new IncrementJob[NUMBER_OF_TASKS];
//
//		System.out.println( "Starting stress tests on " + PARALLEL_THREADS + " Threads running " + NUMBER_OF_TASKS + " tasks" );
//		// Prepare all jobs (quite a lot of array allocations):
//		for ( int i = 0; i < NUMBER_OF_TASKS; i ++ ) {
//			runJobs[i] = new IncrementJob( dialect, nextValueRequest );
//		}
//		// Start them, pretty much in parallel (not really, but we have a lot so they will eventually run in parallel):
//		for ( int i = 0; i < NUMBER_OF_TASKS; i ++ ) {
//			executorService.execute( runJobs[i] );
//		}
//		executorService.shutdown();
//		executorService.awaitTermination( 10, TimeUnit.MINUTES );
//		return runJobs;
//	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Frog.class );
		return configuration;
	}

	@Test
	public void test(TestContext context) {
		Async async = context.async();
		test( async, context, connection()
				.thenCompose( reactiveConnection -> generateId( reactiveConnection, reactiveGenerator() )
						.thenAccept( id -> assertThat( id > 0 ).isTrue() )
						.whenComplete( (unused, throwable) -> reactiveConnection.close() ) );
	}

	private Properties properties() {
		Properties config = new Properties();
		config.setProperty( IdentifierGenerator.ENTITY_NAME, Frog.class.getName() );
		config.setProperty( SequenceStyleGenerator.INCREMENT_PARAM, String.valueOf( SequenceStyleGenerator.DEFAULT_INCREMENT_SIZE ) );
		return config;
	}

	private CompletionStage<Long> generateId(ReactiveConnection reactiveConnection, ReactiveGeneratorWrapper reactiveGenerator ) {
		ReactiveConnectionSupplier supplier = new ReactiveConnectionSupplier() {
			@Override
			public ReactiveConnection getReactiveConnection() {
				return reactiveConnection;
			}
		};

		return reactiveGenerator.generate( supplier( reactiveConnection ), new Frog() );
	}

	private ReactiveGeneratorWrapper reactiveGenerator() {
		ReactiveIdentifierGeneratorFactory identifierGeneratorFactory = getServiceRegistry().getService( ReactiveIdentifierGeneratorFactory.class );
		IdentifierGenerator identifierGenerator = identifierGeneratorFactory.createIdentifierGenerator( SequenceStyleGenerator.class.getName(), LongType.INSTANCE, properties() );
		assertThat( identifierGenerator ).isInstanceOf( ReactiveGeneratorWrapper.class );
		return (ReactiveGeneratorWrapper) identifierGenerator;
	}

	private ReactiveConnectionSupplier supplier(ReactiveConnection reactiveConnection) {
		return new ReactiveConnectionSupplier() {
			@Override
			public ReactiveConnection getReactiveConnection() {
				return reactiveConnection;
			}
		};
	}

	@Entity(name = "Frog")
	public static class Frog {
		@Id
		@GeneratedValue
		public Long id;

		public String name;
	}
}

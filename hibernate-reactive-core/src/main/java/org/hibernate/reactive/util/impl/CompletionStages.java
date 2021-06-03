/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.impl;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;

import com.ibm.asyncutil.iteration.AsyncIterator;

import static com.ibm.asyncutil.iteration.AsyncTrampoline.asyncWhile;

public class CompletionStages {

	private static final CoreMessageLogger log =
			CoreLogging.messageLogger("org.hibernate.reactive.errors");

	// singleton instances:
	private static final CompletionStage<Void> VOID = completedFuture( null );
	private static final CompletionStage<Integer> ZERO = completedFuture( 0 );
	private static final CompletionStage<Boolean> TRUE = completedFuture( true );
	private static final CompletionStage<Boolean> FALSE = completedFuture( false );

	public static CompletionStage<Void> voidFuture(Object ignore) {
		return voidFuture();
	}

	public static CompletionStage<Void> voidFuture() {
		return VOID;
	}

	public static CompletionStage<Integer> zeroFuture() {
		return ZERO;
	}

	public static CompletionStage<Boolean> trueFuture() {
		return TRUE;
	}
	public static CompletionStage<Boolean> falseFuture() {
		return FALSE;
	}

	public static <T> CompletionStage<T> nullFuture() {
		//Unsafe cast, but perfectly fine: avoids having to allocate a new instance
		//for each different "type of null".
		return (CompletionStage<T>) VOID;
	}

	public static <T> CompletionStage<T> completedFuture(T value) {
		return CompletableFuture.completedFuture( value );
	}

	public static <T> CompletionStage<T> failedFuture(Throwable t) {
		CompletableFuture<T> ret = new CompletableFuture<>();
		ret.completeExceptionally( t );
		return ret;
	}

	public static <T extends Throwable, Ret> Ret rethrow(Throwable x) throws T {
		throw (T) x;
	}

	public static <T extends Throwable, Ret> Ret returnNullorRethrow(Throwable x) throws T {
		if ( x != null ) {
			throw (T) x;
		}
		return null;
	}

	public static <T extends Throwable, Ret> Ret returnOrRethrow(Throwable x, Ret result) throws T {
		if ( x != null ) {
			throw (T) x;
		}
		return result;
	}

	public static void logSqlException(Throwable t, Supplier<String> message, String sql) {
		if ( t != null ) {
			log.error( "failed to execute statement [" + sql + "]" );
			log.error( message.get(), t );
		}
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int total = 0;
	 * for ( int i = start; i < end; i++ ) {
	 *   total = total + consumer.apply( i );
	 * }
	 * </pre>
	 */
	public static CompletionStage<Integer> total(int start, int end, IntFunction<CompletionStage<Integer>> consumer) {
		return AsyncIterator.range( start, end )
				.thenCompose( i -> consumer.apply( i.intValue() ) )
				.fold( 0, Integer::sum );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int total = 0;
	 * while( iterator.hasNext() ) {
	 *   total += consumer.apply( iterator.next() );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Integer> total(Iterator<T> iterator, Function<T,CompletionStage<Integer>> consumer) {
		return AsyncIterator.fromIterator( iterator )
				.thenCompose( consumer )
				.fold( 0, Integer::sum );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int total = 0;
	 * for ( int i = start; i < end; i++ ) {
	 *   total = total + consumer.apply( array[i] );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Integer> total(T[] array, Function<T, CompletionStage<Integer>> consumer) {
		return total( 0, array.length, index -> consumer.apply( array[index] ) );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   consumer.apply( array[i] );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(T[] array, Function<T, CompletionStage<?>> consumer) {
		return loop( 0, array.length, index -> consumer.apply( array[index] ) );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   if ( filter.test(i) )  {
	 *   	consumer.apply( i );
	 *   }
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(T[] array, IntPredicate filter, IntFunction<CompletionStage<?>> consumer) {
		return loop( 0, array.length, filter, consumer );
	}

	private static <T> boolean alwaysTrue(T o, int index) {
		return true;
	}

	private static boolean alwaysTrue(int index) {
		return true;
	}

	@FunctionalInterface
	public interface IntBiFunction<T, R> {
		R apply(T value, int integer);
	}

	@FunctionalInterface
	public interface IntBiPredicate<T> {
		boolean test(T value, int integer);
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * while( iterator.hasNext() ) {
	 *   consumer.apply( iterator.next() );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Iterator<T> iterator, Function<T, CompletionStage<?>> consumer) {
		return loop( iterator, CompletionStages::alwaysTrue, (value, integer) -> consumer.apply( value ) );
	}

	public static <T> CompletionStage<Void> loop(Iterator<T> iterator, Predicate<T> filter, Function<T, CompletionStage<?>> consumer) {
		return loop( iterator, (value, integer) -> filter.test( value ), (value, integer) -> consumer.apply( value ) );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int index = 0
	 * while( iterator.hasNext() ) {
	 *   consumer.apply( iterator.next(), index++ );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Iterator<T> iterator, IntBiFunction<T, CompletionStage<?>> consumer) {
		return loop( iterator, CompletionStages::alwaysTrue, consumer );
	}
	/**
	 * Equivalent to:
	 * <pre>
	 * int index = -1
	 * while( iterator.hasNext() ) {
	 *   index++
	 *   T next = iterator.next();
	 *   if (filter.test( next, index ) {
	 *     consumer.apply( next, index );
	 *   }
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Iterator<T> iterator, IntBiPredicate<T> filter, IntBiFunction<T, CompletionStage<?>> consumer) {
		if ( iterator.hasNext() ) {
			final IndexedIteratorLoop<T> loop = new IndexedIteratorLoop<>( iterator, filter, consumer );
			return asyncWhile( loop::next )
					.thenCompose( CompletionStages::voidFuture );
		}
		return voidFuture();
	}

	private static class IndexedIteratorLoop<T> {

		private final IntBiPredicate<T> filter;
		private final IntBiFunction<T, CompletionStage<?>> consumer;
		private final Iterator<T> iterator;
		private boolean hasNext;
		private int currentIndex = -1;
		private T currentEntry;

		public IndexedIteratorLoop(Iterator<T> iterator, IntBiPredicate<T> filter, IntBiFunction<T, CompletionStage<?>> consumer) {
			this.iterator = iterator;
			this.filter = filter;
			this.consumer = consumer;
			filterValues();
		}

		public CompletionStage<Boolean> next() {
			if ( hasNext ) {
				final T entry = currentEntry;
				final int index = currentIndex;
				filterValues();
				return consumer.apply( entry, index )
						.thenCompose( CompletionStages::alwaysContinue );
			}
			return FALSE;
		}

		// Skip all the indexes not matching the filter
		private void filterValues() {
			int index = currentIndex;
			T next = currentEntry;
			this.hasNext = false;
			while ( iterator.hasNext() ) {
				next = iterator.next();
				index++;
				if ( filter.test( next, index ) ) {
					this.hasNext = true;
					break;
				}
			}
			this.currentEntry = next;
			this.currentIndex = index;
		}
	}

	private static CompletionStage<Boolean> alwaysContinue(Object ignored) {
		return TRUE;
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( Object next : iterable ) {
	 *   consumer.apply( next );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Iterable<T> iterable, Function<T,CompletionStage<?>> consumer) {
		return loop( iterable.iterator(), consumer );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * Iterator iterator = stream.iterator();
	 * while( iterator.hasNext() ) {
	 *   consumer.apply( iterator.next() );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Stream<T> stream, Function<T,CompletionStage<?>> consumer) {
		return loop( stream.iterator(), consumer );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   consumer.apply( i );
	 * }
	 * </pre>
	 */
	public static CompletionStage<Void> loop(int start, int end, IntFunction<CompletionStage<?>> consumer) {
		return loop( start, end, CompletionStages::alwaysTrue, consumer );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   if ( filter.test(i) ) {
	 *   	consumer.apply( i );
	 *   }
	 * }
	 * </pre>
	 */
	public static CompletionStage<Void> loop(int start, int end, IntPredicate filter, IntFunction<CompletionStage<?>> consumer) {
		if ( start < end ) {
			final ArrayLoop loop = new ArrayLoop( start, end, filter, consumer);
			return asyncWhile( loop::next )
					.thenCompose( CompletionStages::voidFuture );
		}
		return voidFuture();
	}

	private static class ArrayLoop {

		private final IntPredicate filter;
		private final IntFunction<CompletionStage<?>> consumer;
		private final int end;
		private int current;

		public ArrayLoop(int start, int end, IntPredicate filter, IntFunction<CompletionStage<?>> consumer) {
			this.end = end;
			this.filter = filter;
			this.consumer = consumer;
			this.current = start;
		}

		public CompletionStage<Boolean> next() {
			current = next( current );
			if ( current < end ) {
				final int index = current++;
				return consumer.apply( index )
						.thenCompose( CompletionStages::alwaysContinue );
			}
			return FALSE;
		}

		/**
		 * @param start the first index to test
		 * @return the next valid index
		 */
		private int next(int start) {
			int index = start;
			while ( index < end && !filter.test( index ) ) {
				index++;
			}
			return index;
		}
	}

	public static CompletionStage<Void> applyToAll(Function<Object, CompletionStage<?>> op, Object[] entity) {
		switch ( entity.length ) {
			case 0: return nullFuture();
			case 1: return op.apply( entity[0] ).thenCompose( CompletionStages::voidFuture );
			default: return CompletionStages.loop( entity, op );
		}
	}

	/**
	 * Same as {@link #loop(Iterator, Function)} but doesn't use the trampoline pattern
	 */
	public static <T> CompletionStage<Void> loopWithoutTrampoline(Iterator<T> iterator, Function<T, CompletionStage<?>> consumer) {
		CompletionStage<?> loopStage = voidFuture();
		while ( iterator.hasNext() ) {
			final T next = iterator.next();
			loopStage = loopStage.thenCompose( v -> consumer.apply( next ) );
		}
		return loopStage.thenCompose( CompletionStages::voidFuture );
	}
}

/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.impl;



public interface Trampoline<T> {

	T get();

	default Trampoline<T> jump() {
		return this;
	}

	default T result() {
		return get();
	}

	default boolean complete() {
		return true;
	}

	static <T> Trampoline<T> done(final T result) {
		return () -> result;
	}

	static <T> Trampoline<T> more(final Trampoline<Trampoline<T>> trampoline) {
		return new Trampoline<T>() {
			@Override
			public boolean complete() {
				return false;
			}

			@Override
			public Trampoline<T> jump() {
				return trampoline.result();
			}

			@Override
			public T get() {
				return trampoline( this );
			}

			T trampoline(final Trampoline<T> trampoline) {
				Trampoline<T> next = trampoline;
				while ( !next.complete() ) {
					next = next.jump();
				}
				return next.result();
			}
		};
	}
}

package org.hibernate.reactive.example.session;

import javax.persistence.EntityManagerFactory;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.Root;

import java.sql.ResultSet;
import java.time.LocalDate;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.stage.Stage;

import static java.lang.System.out;
import static java.time.Month.JANUARY;
import static java.time.Month.JUNE;
import static java.time.Month.MAY;
import static javax.persistence.Persistence.createEntityManagerFactory;
import static org.hibernate.reactive.stage.Stage.SessionFactory;
import static org.hibernate.reactive.stage.Stage.fetch;

/**
 * Demonstrates the use of Hibernate Reactive with the
 * {@link java.util.concurrent.CompletionStage}-based
 * API.
 */
public class Main {

	// The first argument can be used to select a persistence unit.
	// Check resources/META-INF/persistence.xml for available names.
	public static void main(String[] args) {
		out.println( "== CompletionStage API Example ==" );

		// obtain a factory for reactive sessions based on the
		// standard JPA configuration properties specified in
		// resources/META-INF/persistence.xml
		EntityManagerFactory emFactory = createEntityManagerFactory( persistenceUnitName( args ) );

		final ReactiveConnectionPool service = emFactory.unwrap( SessionFactoryImplementor.class )
				.getServiceRegistry()
				.getService( ReactiveConnectionPool.class );

		Stage.SessionFactory factory = emFactory.unwrap( Stage.SessionFactory.class );

		// define some test data
		Author author1 = new Author("Iain M. Banks");
		Author author2 = new Author("Neal Stephenson");
		Book book1 = new Book("1-85723-235-6", "Feersum Endjinn", author1, LocalDate.of(1994, JANUARY, 1));
		Book book2 = new Book("0-380-97346-4", "Cryptonomicon", author2, LocalDate.of(1999, MAY, 1));
		Book book3 = new Book("0-553-08853-X", "Snow Crash", author2, LocalDate.of(1992, JUNE, 1));
		author1.getBooks().add(book1);
		author2.getBooks().add(book2);
		author2.getBooks().add(book3);

		try {
			// obtain a reactive session
			factory.withTransaction(
					// persist the Authors with their Books in a transaction
					(session, tx) -> session.persist( author1, author2 )
			)
					// wait for it to finish
					.toCompletableFuture().join();


			final ResultSet join = service.getConnection()
					.thenCompose( c -> c.selectJdbc( "select * from authors", new Object[0] ) )
					.toCompletableFuture()
					.join();

			out.println(join);
		}
		finally {
			// remember to shut down the factory
			factory.close();
		}
	}

	/**
	 * Return the persistence unit name to use in the example.
	 *
	 * @param args the first element is the persistence unit name if present
	 * @return the selected persistence unit name or the default one
	 */
	public static String persistenceUnitName(String[] args) {
		return args.length > 0 ? args[0] : "postgresql-example";
	}
}

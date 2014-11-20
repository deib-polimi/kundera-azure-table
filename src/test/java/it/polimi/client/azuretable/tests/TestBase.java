package it.polimi.client.azuretable.tests;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Fabio Arcidiacono.
 */
@Slf4j
public abstract class TestBase {

    /** AzureTable testing tools */
    // TODO

    /** JPA stuff */
    private static final String PERSISTENCE_UNIT = "pu";
    private EntityManagerFactory emf;
    protected EntityManager em;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        // TODO azure setup
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        if (em != null && em.isOpen()) {
            em.close();
        }
        em = emf.createEntityManager();
    }

    @After
    public void tearDown() {
        em.close();
        emf.close();
        // TODO azure tear down
    }

    /*---------------------------------------------------------------------------------*/
    /*-------------------------- UTILS, for debug purposes ----------------------------*/

    protected void clear() {
        em.clear();
        print("clear entity manager");
    }

    protected void print(String message) {
        if (log.isDebugEnabled()) {
            String delimiter = "--------------------------------------------------------------------";
            String spacing = message.length() <= 10 ? "\t\t\t\t\t\t\t  " : "\t\t\t\t\t\t";
            log.debug("\n" + delimiter + "\n" + spacing + message.toUpperCase() + "\n" + delimiter);
        } else {
            log.info("\t\t" + message.toUpperCase() + "\n");
        }
    }
}

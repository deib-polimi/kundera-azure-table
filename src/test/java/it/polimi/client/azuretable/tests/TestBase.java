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

    /*
     * AzureTable testing can be done on cloud since Microsoft does not provide an in-memory stub for Table service.
     * It is possible (on Windows) to test over Azure Storage Emulator which emulates Table service
     * over a Microsoft SQL server, differences in behavior are listed at
     * http://msdn.microsoft.com/en-us/library/azure/gg433135.aspx
     */

    /** JPA stuff */
    private static final String PERSISTENCE_UNIT = "pu";
    private EntityManagerFactory emf;
    protected EntityManager em;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        if (em != null && em.isOpen()) {
            em.close();
        }
        em = emf.createEntityManager();
    }

    @After
    public void tearDown() {
        if (em != null) {
            em.close();
        }
        if (emf != null) {
            emf.close();
        }
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

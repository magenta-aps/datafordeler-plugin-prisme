package dk.magenta.datafordeler.prisme;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.core.io.ImportMetadata;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.InputStreamReader;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cvr.CvrPlugin;
import dk.magenta.datafordeler.cvr.access.CvrAreaRestrictionDefinition;
import dk.magenta.datafordeler.cvr.access.CvrRolesDefinition;
import dk.magenta.datafordeler.cvr.entitymanager.CompanyEntityManager;
import dk.magenta.datafordeler.geo.GeoPlugin;
import dk.magenta.datafordeler.ger.GerPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import static org.mockito.Mockito.when;

/**
 * Test the service for combined lookup locally and remote.
 * The unittest is not actually trying to lookup companys that is not stored locally.
 * This is becrause we do not want the unittest to access outside webservices and use passwords etc.
 * The unittest could be expanded with a mockup of the external cvr-server
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CvrCombinedTest extends TestBase {

    private Logger log = LogManager.getLogger(CvrCombinedTest.class.getCanonicalName());

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private CompanyEntityManager companyEntityManager;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    TestRestTemplate restTemplate;

    @SpyBean
    private DafoUserManager dafoUserManager;

    @Autowired
    private CvrPlugin cvrPlugin;

    @Autowired
    private GerPlugin gerPlugin;


    @After
    public void cleanup() {
        this.cleanupCompanyData(sessionManager);
        this.cleanupGeoData(sessionManager);
    }



    @Test
    public void testCompanyPrisme() throws IOException, DataFordelerException {
        this.loadGerCompany(gerPlugin, sessionManager);
        this.loadGerParticipant(gerPlugin, sessionManager);
        this.loadAllGeoAdress(sessionManager);
        this.loadCompany(cvrPlugin, sessionManager, objectMapper);


            TestUserDetails testUserDetails = new TestUserDetails();


            HttpEntity<String> httpEntity = new HttpEntity<String>("", new HttpHeaders());
            ResponseEntity<String> response = restTemplate.exchange(
                    "/prisme/cvr/3/" + 25052943,
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());


            testUserDetails.giveAccess(CvrRolesDefinition.READ_CVR_ROLE);
            this.applyAccess(testUserDetails);
            response = restTemplate.exchange(
                    "/prisme/cvr/3/" + 25052943,
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());


            testUserDetails.giveAccess(
                    cvrPlugin.getAreaRestrictionDefinition().getAreaRestrictionTypeByName(
                            CvrAreaRestrictionDefinition.RESTRICTIONTYPE_KOMMUNEKODER
                    ).getRestriction(
                            CvrAreaRestrictionDefinition.RESTRICTION_KOMMUNE_SERMERSOOQ
                    )
            );
            this.applyAccess(testUserDetails);
            response = restTemplate.exchange(
                    "/prisme/cvr/3/" + 25052943,
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());


            testUserDetails.giveAccess(
                    cvrPlugin.getAreaRestrictionDefinition().getAreaRestrictionTypeByName(
                            CvrAreaRestrictionDefinition.RESTRICTIONTYPE_KOMMUNEKODER
                    ).getRestriction(
                            CvrAreaRestrictionDefinition.RESTRICTION_KOMMUNE_KUJALLEQ
                    )
            );
            testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
            this.applyAccess(testUserDetails);
            response = restTemplate.exchange(
                    "/prisme/cvr/3/" + 25052943 + "?returnParticipantDetails=1",
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());

            response = restTemplate.exchange(
                    "/prisme/cvr/3/" + 1234,
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }



    @Test
    public void testGerFallback() throws IOException, DataFordelerException {
        TestUserDetails testUserDetails = new TestUserDetails();
        this.loadGerCompany(gerPlugin, sessionManager);
        this.loadGerParticipant(gerPlugin, sessionManager);
        this.loadAllGeoAdress(sessionManager);

        testUserDetails.giveAccess(
                cvrPlugin.getAreaRestrictionDefinition().getAreaRestrictionTypeByName(
                        CvrAreaRestrictionDefinition.RESTRICTIONTYPE_KOMMUNEKODER
                ).getRestriction(
                        CvrAreaRestrictionDefinition.RESTRICTION_KOMMUNE_KUJALLEQ
                )
        );
        testUserDetails.giveAccess(CvrRolesDefinition.READ_CVR_ROLE);
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        this.applyAccess(testUserDetails);
        HttpEntity<String> httpEntity = new HttpEntity<String>("", new HttpHeaders());
        ResponseEntity<String> response;
        response = restTemplate.exchange(
                "/prisme/cvr/3/" + 12345678 + "?returnParticipantDetails=1",
                HttpMethod.GET,
                httpEntity,
                String.class
        );
        log.debug(response.getBody());
    }


    @Test
    public void testCompanyBulkPrisme() throws Exception {

        OffsetDateTime start = OffsetDateTime.now();
        loadManyCompanies(cvrPlugin, sessionManager, 5, 0);
        OffsetDateTime middle = OffsetDateTime.now();
        Thread.sleep(10);
        loadManyCompanies(cvrPlugin, sessionManager, 5, 5);

        OffsetDateTime companyUpdate = OffsetDateTime.parse("2017-04-10T09:01:06.000+02:00");

        loadAllGeoAdress(sessionManager);
        OffsetDateTime afterLoad = OffsetDateTime.now();

            TestUserDetails testUserDetails = new TestUserDetails();


            testUserDetails.giveAccess(CvrRolesDefinition.READ_CVR_ROLE);
            this.applyAccess(testUserDetails);

            ObjectNode body = objectMapper.createObjectNode();
            body.put("cvrNumber", "10000009");
            HttpEntity<String> httpEntity = new HttpEntity<>(body.toString(), new HttpHeaders());
            ResponseEntity<String> response = restTemplate.exchange(
                    "/prisme/cvr/3/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(1, objectMapper.readTree(response.getBody()).size());


            body = objectMapper.createObjectNode();
            ArrayNode cvrList = objectMapper.createArrayNode();
            cvrList.add("10000002");
            cvrList.add("10000005");
            body.set("cvrNumber", cvrList);
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            response = restTemplate.exchange(
                    "/prisme/cvr/3/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(2, objectMapper.readTree(response.getBody()).size());



            body = objectMapper.createObjectNode();
            cvrList = objectMapper.createArrayNode();
            cvrList.add("10000000");
            cvrList.add("10000001");
            cvrList.add("10000002");
            cvrList.add("10000003");
            cvrList.add("10000004");
            cvrList.add("10000005");
            cvrList.add("10000006");
            cvrList.add("10000007");
            cvrList.add("10000008");
            cvrList.add("10000009");
            body.set("cvrNumber", cvrList);
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            long tic = Instant.now().toEpochMilli();
            response = restTemplate.exchange(
                    "/prisme/cvr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(10, objectMapper.readTree(response.getBody()).size());



            body = objectMapper.createObjectNode();
            cvrList = objectMapper.createArrayNode();
            cvrList.add("10000000");
            cvrList.add("10000001");
            cvrList.add("10000002");
            cvrList.add("10000003");
            cvrList.add("10000004");
            cvrList.add("10000005");
            cvrList.add("10000006");
            cvrList.add("10000007");
            cvrList.add("10000008");
            cvrList.add("10000009");
            body.set("cvrNumber", cvrList);
            //body.put("updatedSince", start.minusSeconds(1).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            body.put("updatedSince", companyUpdate.minusSeconds(1).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            response = restTemplate.exchange(
                    "/prisme/cvr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(10, objectMapper.readTree(response.getBody()).size());



            body = objectMapper.createObjectNode();
            cvrList = objectMapper.createArrayNode();
            cvrList.add("10000000");
            cvrList.add("10000001");
            cvrList.add("10000002");
            cvrList.add("10000003");
            cvrList.add("10000004");
            cvrList.add("10000005");
            cvrList.add("10000006");
            cvrList.add("10000007");
            cvrList.add("10000008");
            cvrList.add("10000009");
            body.set("cvrNumber", cvrList);
            //body.put("updatedSince", afterLoad.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            body.put("updatedSince", companyUpdate.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            response = restTemplate.exchange(
                    "/prisme/cvr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            log.debug(response.getBody());
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(0, objectMapper.readTree(response.getBody()).size());
    }

    private void applyAccess(TestUserDetails testUserDetails) {
        when(dafoUserManager.getFallbackUser()).thenReturn(testUserDetails);
    }

}

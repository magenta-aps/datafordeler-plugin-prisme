package dk.magenta.datafordeler.prisme;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.*;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.core.io.ImportMetadata;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.InputStreamReader;
import dk.magenta.datafordeler.cpr.CprAreaRestrictionDefinition;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.gladdrreg.GladdrregPlugin;
import dk.magenta.datafordeler.gladdrreg.data.locality.*;
import dk.magenta.datafordeler.gladdrreg.data.municipality.*;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.*;
import dk.magenta.datafordeler.gladdrreg.data.road.*;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.StringJoiner;

import static org.mockito.Mockito.when;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CprTest {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private PersonEntityManager personEntityManager;

    @Autowired
    private GladdrregPlugin gladdrregPlugin;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @SpyBean
    private DafoUserManager dafoUserManager;

    @Autowired
    private CprPlugin cprPlugin;

    HashSet<Entity> createdEntities = new HashSet<>();

    public void loadPerson() throws Exception {
        InputStream testData = CprTest.class.getResourceAsStream("/person.txt");
        ImportMetadata importMetadata = new ImportMetadata();
        List<PersonRegistration> registrations = personEntityManager.parseRegistration(testData, importMetadata);
        testData.close();
        for (PersonRegistration registration : registrations) {
            createdEntities.add(registration.getEntity());
        }
    }

    public void loadManyPersons(int count) throws Exception {
        this.loadManyPersons(count, 0);
    }

    public void loadManyPersons(int count, int start) throws Exception {
        ImportMetadata importMetadata = new ImportMetadata();
        String testData = InputStreamReader.readInputStream(CprTest.class.getResourceAsStream("/person.txt"));
        String[] lines = testData.split("\n");
        for (int i = start; i < count + start; i++) {
            StringJoiner sb = new StringJoiner("\n");
            String newCpr = String.format("%010d", i);
            for (int j = 0; j < lines.length; j++) {
                String line = lines[j];
                line = line.substring(0, 3) + newCpr + line.substring(13);
                sb.add(line);
            }
            ByteArrayInputStream bais = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
            List<PersonRegistration> registrations = personEntityManager.parseRegistration(bais, importMetadata);
            bais.close();
            /*for (PersonRegistration registration : registrations) {
                createdEntities.add(registration.getEntity());
            }*/
        }
    }

    private void loadLocality(Session session) throws DataFordelerException, IOException {
        InputStream testData = CprTest.class.getResourceAsStream("/locality.json");
        LocalityEntityManager localityEntityManager = (LocalityEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(LocalityEntity.schema);
        List<? extends Registration> regs = localityEntityManager.parseRegistration(testData, new ImportMetadata());
        testData.close();
        for (Registration registration : regs) {
            LocalityRegistration localityRegistration = (LocalityRegistration) registration;
            QueryManager.saveRegistration(session, localityRegistration.getEntity(), localityRegistration);
            createdEntities.add(localityRegistration.getEntity());
        }
    }

    private void loadRoad(Session session) throws DataFordelerException, IOException {
        InputStream testData = CprTest.class.getResourceAsStream("/road.json");
        RoadEntityManager roadEntityManager = (RoadEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(RoadEntity.schema);
        List<? extends Registration> regs = roadEntityManager.parseRegistration(testData, new ImportMetadata());
        testData.close();
        for (Registration registration : regs) {
            RoadRegistration roadRegistration = (RoadRegistration) registration;
            QueryManager.saveRegistration(session, roadRegistration.getEntity(), roadRegistration);
            createdEntities.add(roadRegistration.getEntity());
        }
    }

    private void loadMunicipality(Session session) throws DataFordelerException, IOException {
        InputStream testData = CprTest.class.getResourceAsStream("/municipality.json");
        MunicipalityEntityManager municipalityEntityManager = (MunicipalityEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(MunicipalityEntity.schema);
        List<? extends Registration> regs = municipalityEntityManager.parseRegistration(testData, new ImportMetadata());
        testData.close();
        for (Registration registration : regs) {
            MunicipalityRegistration municipalityRegistration = (MunicipalityRegistration) registration;
            QueryManager.saveRegistration(session, municipalityRegistration.getEntity(), municipalityRegistration);
            createdEntities.add(municipalityRegistration.getEntity());
        }
    }

    private void loadPostalCode(Session session) throws DataFordelerException {
        InputStream testData = CprTest.class.getResourceAsStream("/postalcode.json");
        PostalCodeEntityManager postalCodeEntityManager = (PostalCodeEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(PostalCodeEntity.schema);
        List<? extends Registration> regs = postalCodeEntityManager.parseRegistration(testData, new ImportMetadata());
        for (Registration registration : regs) {
            PostalCodeRegistration postalCodeRegistration = (PostalCodeRegistration) registration;
            QueryManager.saveRegistration(session, postalCodeRegistration.getEntity(), postalCodeRegistration);
            createdEntities.add(postalCodeRegistration.getEntity());
        }
    }

    private void loadGladdrregData() throws IOException, DataFordelerException {
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            Transaction transaction = session.beginTransaction();
            loadLocality(session);
            loadRoad(session);
            loadMunicipality(session);
            loadPostalCode(session);
            transaction.commit();
        } finally {
            session.close();
        }
    }

    @Test
    public void testPersonPrisme() throws Exception {
        loadPerson();
        loadGladdrregData();

        try {
            TestUserDetails testUserDetails = new TestUserDetails();


            HttpEntity<String> httpEntity = new HttpEntity<String>("", new HttpHeaders());
            ResponseEntity<String> response = restTemplate.exchange(
                    "/prisme/cpr/1/" + "0101001234",
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());


            testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
            this.applyAccess(testUserDetails);

            response = restTemplate.exchange(
                    "/prisme/cpr/1/" + "0101001234",
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertTrue(objectMapper.readTree(response.getBody()).size() > 0);



            testUserDetails.giveAccess(
                    cprPlugin.getAreaRestrictionDefinition().getAreaRestrictionTypeByName(
                            CprAreaRestrictionDefinition.RESTRICTIONTYPE_KOMMUNEKODER
                    ).getRestriction(
                            CprAreaRestrictionDefinition.RESTRICTION_KOMMUNE_SERMERSOOQ
                    )
            );
            this.applyAccess(testUserDetails);
            response = restTemplate.exchange(
                    "/prisme/cpr/1/" + "0101001234",
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());


            testUserDetails.giveAccess(
                    cprPlugin.getAreaRestrictionDefinition().getAreaRestrictionTypeByName(
                            CprAreaRestrictionDefinition.RESTRICTIONTYPE_KOMMUNEKODER
                    ).getRestriction(
                            CprAreaRestrictionDefinition.RESTRICTION_KOMMUNE_KUJALLEQ
                    )
            );
            this.applyAccess(testUserDetails);
            response = restTemplate.exchange(
                    "/prisme/cpr/1/" + "0101001234",
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertTrue(objectMapper.readTree(response.getBody()).size() > 0);

            System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectMapper.readTree(response.getBody())));

        } finally {
            cleanup();
        }
    }

    @Test
    public void testPersonBulkPrisme() throws Exception {

        OffsetDateTime start = OffsetDateTime.now();
        loadManyPersons(5, 0);
        OffsetDateTime middle = OffsetDateTime.now();
        Thread.sleep(10);
        loadManyPersons(5, 5);
        OffsetDateTime afterLoad = OffsetDateTime.now();

        loadGladdrregData();

        try {
            TestUserDetails testUserDetails = new TestUserDetails();


            testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
            this.applyAccess(testUserDetails);

            ObjectNode body = objectMapper.createObjectNode();
            body.put("cprNumber", "0000000009");
            HttpEntity<String> httpEntity = new HttpEntity<>(body.toString(), new HttpHeaders());
            ResponseEntity<String> response = restTemplate.exchange(
                    "/prisme/cpr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(1, objectMapper.readTree(response.getBody()).size());


            body = objectMapper.createObjectNode();
            ArrayNode cprList = objectMapper.createArrayNode();
            cprList.add("0000000002");
            cprList.add("0000000005");
            body.set("cprNumber", cprList);
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            response = restTemplate.exchange(
                    "/prisme/cpr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(2, objectMapper.readTree(response.getBody()).size());




            body = objectMapper.createObjectNode();
            cprList = objectMapper.createArrayNode();
            cprList.add("0000000000");
            cprList.add("0000000001");
            cprList.add("0000000002");
            cprList.add("0000000003");
            cprList.add("0000000004");
            cprList.add("0000000005");
            cprList.add("0000000006");
            cprList.add("0000000007");
            cprList.add("0000000008");
            cprList.add("0000000009");
            body.set("cprNumber", cprList);
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            response = restTemplate.exchange(
                    "/prisme/cpr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(10, objectMapper.readTree(response.getBody()).size());



            body = objectMapper.createObjectNode();
            cprList = objectMapper.createArrayNode();
            cprList.add("0000000000");
            cprList.add("0000000001");
            cprList.add("0000000002");
            cprList.add("0000000003");
            cprList.add("0000000004");
            cprList.add("0000000005");
            cprList.add("0000000006");
            cprList.add("0000000007");
            cprList.add("0000000008");
            cprList.add("0000000009");
            body.set("cprNumber", cprList);
            body.put("updatedSince", start.minusSeconds(1).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            response = restTemplate.exchange(
                    "/prisme/cpr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(10, objectMapper.readTree(response.getBody()).size());



            body = objectMapper.createObjectNode();
            cprList = objectMapper.createArrayNode();
            cprList.add("0000000000");
            cprList.add("0000000001");
            cprList.add("0000000002");
            cprList.add("0000000003");
            cprList.add("0000000004");
            cprList.add("0000000005");
            cprList.add("0000000006");
            cprList.add("0000000007");
            cprList.add("0000000008");
            cprList.add("0000000009");
            body.set("cprNumber", cprList);
            body.put("updatedSince", afterLoad.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            response = restTemplate.exchange(
                    "/prisme/cpr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(0, objectMapper.readTree(response.getBody()).size());





            body = objectMapper.createObjectNode();
            cprList = objectMapper.createArrayNode();
            cprList.add("0000000000");
            cprList.add("0000000001");
            cprList.add("0000000002");
            cprList.add("0000000003");
            cprList.add("0000000004");
            cprList.add("0000000005");
            cprList.add("0000000006");
            cprList.add("0000000007");
            cprList.add("0000000008");
            cprList.add("0000000009");
            body.set("cprNumber", cprList);
            body.put("updatedSince", middle.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            response = restTemplate.exchange(
                    "/prisme/cpr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(5, objectMapper.readTree(response.getBody()).size());

        } finally {
            cleanup();
        }
    }

    private void applyAccess(TestUserDetails testUserDetails) {
        when(dafoUserManager.getFallbackUser()).thenReturn(testUserDetails);
    }

    private void cleanup() {
        Session session = sessionManager.getSessionFactory().openSession();
        Transaction transaction = session.beginTransaction();
        try {
            for (Entity entity : createdEntities) {
                session.delete(entity);
            }
            createdEntities.clear();
        } finally {
            try {
                transaction.commit();
            } catch (Exception e) {
            } finally {
                session.close();
            }
        }
    }

}

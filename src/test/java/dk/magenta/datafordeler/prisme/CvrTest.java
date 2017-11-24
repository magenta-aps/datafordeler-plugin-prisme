package dk.magenta.datafordeler.prisme;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.*;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.core.io.ImportMetadata;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.InputStreamReader;
import dk.magenta.datafordeler.cvr.CvrAreaRestrictionDefinition;
import dk.magenta.datafordeler.cvr.CvrPlugin;
import dk.magenta.datafordeler.cvr.CvrRolesDefinition;
import dk.magenta.datafordeler.cvr.data.company.CompanyEntityManager;
import dk.magenta.datafordeler.gladdrreg.GladdrregPlugin;
import dk.magenta.datafordeler.gladdrreg.data.locality.*;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntityManager;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityRegistration;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CvrTest {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private CompanyEntityManager companyEntityManager;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private GladdrregPlugin gladdrregPlugin;

    @Autowired
    TestRestTemplate restTemplate;

    @SpyBean
    private DafoUserManager dafoUserManager;

    @Autowired
    private CvrPlugin cvrPlugin;

    HashSet<Entity> createdEntities = new HashSet<>();


    private void loadCompany() throws IOException, DataFordelerException {
        InputStream testData = CvrTest.class.getResourceAsStream("/company_in.json");
        JsonNode root = objectMapper.readTree(testData);
        testData.close();
        JsonNode itemList = root.get("hits").get("hits");
        Assert.assertTrue(itemList.isArray());
        ImportMetadata importMetadata = new ImportMetadata();
        for (JsonNode item : itemList) {
            String source = objectMapper.writeValueAsString(item.get("_source").get("Vrvirksomhed"));
            ByteArrayInputStream bais = new ByteArrayInputStream(source.getBytes("UTF-8"));
            List<? extends Registration> registrations = companyEntityManager.parseRegistration(bais, importMetadata);
            bais.close();
            /*for (Registration registration : registrations) {
                createdEntities.add(registration.getEntity());
            }*/
        }
    }

    public void loadManyCompanies(int count) throws Exception {
        this.loadManyCompanies(count, 0);
    }

    public void loadManyCompanies(int count, int start) throws Exception {
        ImportMetadata importMetadata = new ImportMetadata();
        String testData = InputStreamReader.readInputStream(CvrTest.class.getResourceAsStream("/company_in.json"));
        for (int i = start; i < count + start; i++) {
            String altered = testData.replaceAll("25052943", "1" + String.format("%07d", i));
            ByteArrayInputStream bais = new ByteArrayInputStream(altered.getBytes("UTF-8"));
            List<? extends Registration> registrations = companyEntityManager.parseRegistration(bais, importMetadata);
            bais.close();
            /*for (Registration registration : registrations) {
                createdEntities.add(registration.getEntity());
            }*/
        }
    }

    private void loadLocality(Session session) throws DataFordelerException, IOException {
        InputStream testData = CvrTest.class.getResourceAsStream("/locality.json");
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
        InputStream testData = CvrTest.class.getResourceAsStream("/road.json");
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
        InputStream testData = CvrTest.class.getResourceAsStream("/municipality.json");
        MunicipalityEntityManager municipalityEntityManager = (MunicipalityEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(MunicipalityEntity.schema);
        List<? extends Registration> regs = municipalityEntityManager.parseRegistration(testData, new ImportMetadata());
        testData.close();
        for (Registration registration : regs) {
            MunicipalityRegistration municipalityRegistration = (MunicipalityRegistration) registration;
            QueryManager.saveRegistration(session, municipalityRegistration.getEntity(), municipalityRegistration);
            createdEntities.add(municipalityRegistration.getEntity());
        }
    }

    private void loadGladdrregData() throws IOException, DataFordelerException {
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            Transaction transaction = session.beginTransaction();
            loadLocality(session);
            loadRoad(session);
            loadMunicipality(session);
            transaction.commit();
        } finally {
            session.close();
        }
    }

    @Test
    public void testCompanyPrisme() throws IOException, DataFordelerException {
        loadGladdrregData();
        loadCompany();

        try {

            TestUserDetails testUserDetails = new TestUserDetails();


            HttpEntity<String> httpEntity = new HttpEntity<String>("", new HttpHeaders());
            ResponseEntity<String> response = restTemplate.exchange(
                    "/prisme/cvr/1/" + 25052943,
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());


            testUserDetails.giveAccess(CvrRolesDefinition.READ_CVR_ROLE);
            this.applyAccess(testUserDetails);
            response = restTemplate.exchange(
                    "/prisme/cvr/1/" + 25052943,
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
                    "/prisme/cvr/1/" + 25052943,
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());


            testUserDetails.giveAccess(
                    cvrPlugin.getAreaRestrictionDefinition().getAreaRestrictionTypeByName(
                            CvrAreaRestrictionDefinition.RESTRICTIONTYPE_KOMMUNEKODER
                    ).getRestriction(
                            CvrAreaRestrictionDefinition.RESTRICTION_KOMMUNE_KUJALLEQ
                    )
            );
            this.applyAccess(testUserDetails);
            response = restTemplate.exchange(
                    "/prisme/cvr/1/" + 25052943,
                    HttpMethod.GET,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());

        } finally {
            cleanup();
        }
    }


    @Test
    public void testCompanyBulkPrisme() throws Exception {

        OffsetDateTime start = OffsetDateTime.now();
        loadManyCompanies(5, 0);
        OffsetDateTime middle = OffsetDateTime.now();
        Thread.sleep(10);
        loadManyCompanies(5, 5);

        loadGladdrregData();
        OffsetDateTime afterLoad = OffsetDateTime.now();

        try {
            TestUserDetails testUserDetails = new TestUserDetails();


            testUserDetails.giveAccess(CvrRolesDefinition.READ_CVR_ROLE);
            this.applyAccess(testUserDetails);

            ObjectNode body = objectMapper.createObjectNode();
            body.put("cvrNumber", "10000009");
            HttpEntity<String> httpEntity = new HttpEntity<>(body.toString(), new HttpHeaders());
            ResponseEntity<String> response = restTemplate.exchange(
                    "/prisme/cvr/1/",
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
                    "/prisme/cvr/1/",
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
            body.put("updatedSince", start.minusSeconds(1).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
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
            body.put("updatedSince", afterLoad.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            response = restTemplate.exchange(
                    "/prisme/cvr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(0, objectMapper.readTree(response.getBody()).size());





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
            body.put("updatedSince", middle.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            httpEntity = new HttpEntity<String>(body.toString(), new HttpHeaders());
            response = restTemplate.exchange(
                    "/prisme/cvr/1/",
                    HttpMethod.POST,
                    httpEntity,
                    String.class
            );
            Assert.assertEquals(HttpStatus.OK, response.getStatusCode());
            Assert.assertEquals(5, objectMapper.readTree(response.getBody()).size());

            System.out.println(response);
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
                try {
                    session.delete(entity);
                } catch (Exception e) {}
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

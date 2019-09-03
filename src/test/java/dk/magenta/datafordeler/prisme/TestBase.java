package dk.magenta.datafordeler.prisme;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dk.magenta.datafordeler.core.database.DatabaseEntry;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.core.io.ImportMetadata;
import dk.magenta.datafordeler.core.util.InputStreamReader;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cvr.CvrPlugin;
import dk.magenta.datafordeler.cvr.records.CompanyRecord;
import dk.magenta.datafordeler.ger.GerPlugin;
import dk.magenta.datafordeler.ger.data.company.CompanyEntity;
import dk.magenta.datafordeler.ger.data.responsible.ResponsibleEntity;
import dk.magenta.datafordeler.gladdrreg.GladdrregPlugin;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEntityManager;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityRegistration;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntityManager;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityRegistration;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeEntity;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeEntityManager;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeRegistration;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadEntity;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadEntityManager;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadRegistration;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public abstract class TestBase {

    protected void cleanup(SessionManager sessionManager, Class[] classes) {
        Session session = sessionManager.getSessionFactory().openSession();
        Transaction transaction = session.beginTransaction();
        try {
            for (Class cls : classes) {
                List<DatabaseEntry> eList = QueryManager.getAllItems(session, cls);
                for (DatabaseEntry e : eList) {
                    session.delete(e);
                }
            }
            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            session.close();
        }
    }

    private void loadLocality(GladdrregPlugin gladdrregPlugin, Session session) throws DataFordelerException, IOException {
        InputStream testData = TestBase.class.getResourceAsStream("/locality.json");
        LocalityEntityManager localityEntityManager = (LocalityEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(LocalityEntity.schema);
        List<? extends Registration> regs = localityEntityManager.parseData(testData, new ImportMetadata());
        testData.close();
        for (Registration registration : regs) {
            LocalityRegistration localityRegistration = (LocalityRegistration) registration;
            QueryManager.saveRegistration(session, localityRegistration.getEntity(), localityRegistration);
        }
    }

    private void loadRoad(GladdrregPlugin gladdrregPlugin, Session session) throws DataFordelerException, IOException {
        InputStream testData = TestBase.class.getResourceAsStream("/road.json");
        RoadEntityManager roadEntityManager = (RoadEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(RoadEntity.schema);
        List<? extends Registration> regs = roadEntityManager.parseData(testData, new ImportMetadata());
        testData.close();
        for (Registration registration : regs) {
            RoadRegistration roadRegistration = (RoadRegistration) registration;
            QueryManager.saveRegistration(session, roadRegistration.getEntity(), roadRegistration);
        }
    }

    private void loadMunicipality(GladdrregPlugin gladdrregPlugin, Session session) throws DataFordelerException, IOException {
        InputStream testData = TestBase.class.getResourceAsStream("/municipality.json");
        MunicipalityEntityManager municipalityEntityManager = (MunicipalityEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(MunicipalityEntity.schema);
        List<? extends Registration> regs = municipalityEntityManager.parseData(testData, new ImportMetadata());
        testData.close();
        for (Registration registration : regs) {
            MunicipalityRegistration municipalityRegistration = (MunicipalityRegistration) registration;
            QueryManager.saveRegistration(session, municipalityRegistration.getEntity(), municipalityRegistration);
        }
    }

    private void loadPostalCode(GladdrregPlugin gladdrregPlugin, Session session) throws DataFordelerException {
        InputStream testData = TestBase.class.getResourceAsStream("/postalcode.json");
        PostalCodeEntityManager postalCodeEntityManager = (PostalCodeEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(PostalCodeEntity.schema);
        List<? extends Registration> regs = postalCodeEntityManager.parseData(testData, new ImportMetadata());
        for (Registration registration : regs) {
            PostalCodeRegistration postalCodeRegistration = (PostalCodeRegistration) registration;
            QueryManager.saveRegistration(session, postalCodeRegistration.getEntity(), postalCodeRegistration);
        }
    }

    protected void loadGladdrregData(GladdrregPlugin gladdrregPlugin, SessionManager sessionManager) throws IOException, DataFordelerException {
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            Transaction transaction = session.beginTransaction();
            loadMunicipality(gladdrregPlugin, session);
            loadLocality(gladdrregPlugin, session);
            loadRoad(gladdrregPlugin, session);
            loadPostalCode(gladdrregPlugin, session);
            transaction.commit();
        } finally {
            session.close();
        }
    }

    protected void loadCompany(CvrPlugin cvrPlugin, SessionManager sessionManager, ObjectMapper objectMapper) throws IOException, DataFordelerException {
        InputStream testData = CvrCombinedTest.class.getResourceAsStream("/company_in.json");
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            dk.magenta.datafordeler.cvr.entitymanager.CompanyEntityManager companyEntityManager = (dk.magenta.datafordeler.cvr.entitymanager.CompanyEntityManager) cvrPlugin.getRegisterManager().getEntityManager(CompanyRecord.schema);
            JsonNode root = objectMapper.readTree(testData);
            testData.close();
            JsonNode itemList = root.get("hits").get("hits");
            Assert.assertTrue(itemList.isArray());
            ImportMetadata importMetadata = new ImportMetadata();
            importMetadata.setSession(session);

            for (JsonNode item : itemList) {
                String source = objectMapper.writeValueAsString(item.get("_source").get("Vrvirksomhed"));
                ByteArrayInputStream bais = new ByteArrayInputStream(source.getBytes("UTF-8"));
                companyEntityManager.parseData(bais, importMetadata);
                bais.close();
            }
        } finally {
            session.close();
        }
    }

    protected void loadManyCompanies(CvrPlugin cvrPlugin, SessionManager sessionManager, int count) throws Exception {
        this.loadManyCompanies(cvrPlugin, sessionManager, count, 0);
    }

    protected void loadManyCompanies(CvrPlugin cvrPlugin, SessionManager sessionManager, int count, int start) throws Exception {
        dk.magenta.datafordeler.cvr.entitymanager.CompanyEntityManager companyEntityManager = (dk.magenta.datafordeler.cvr.entitymanager.CompanyEntityManager) cvrPlugin.getRegisterManager().getEntityManager(CompanyRecord.schema);
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            ImportMetadata importMetadata = new ImportMetadata();
            importMetadata.setSession(session);
            String testData = InputStreamReader.readInputStream(CvrCombinedTest.class.getResourceAsStream("/company_in.json"));
            for (int i = start; i < count + start; i++) {
                String altered = testData.replaceAll("25052943", "1" + String.format("%07d", i)).replaceAll("\n", "");
                ByteArrayInputStream bais = new ByteArrayInputStream(altered.getBytes("UTF-8"));
                companyEntityManager.parseData(bais, importMetadata);
                bais.close();
            }
        } finally {
            session.close();
        }
    }

    protected void loadGerCompany(GerPlugin gerPlugin, SessionManager sessionManager) throws IOException, DataFordelerException {
        InputStream testData = CvrCombinedTest.class.getResourceAsStream("/GER.test.xlsx");
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            dk.magenta.datafordeler.ger.data.company.CompanyEntityManager companyEntityManager = (dk.magenta.datafordeler.ger.data.company.CompanyEntityManager) gerPlugin.getRegisterManager().getEntityManager(CompanyEntity.schema);
            ImportMetadata importMetadata = new ImportMetadata();
            importMetadata.setSession(session);
            companyEntityManager.parseData(testData, importMetadata);
        } finally {
            session.close();
            testData.close();
        }
    }

    protected void loadGerParticipant(GerPlugin gerPlugin, SessionManager sessionManager) throws IOException, DataFordelerException {
        InputStream testData = CvrCombinedTest.class.getResourceAsStream("/GER.test.xlsx");
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            dk.magenta.datafordeler.ger.data.responsible.ResponsibleEntityManager responsibleEntityManager = (dk.magenta.datafordeler.ger.data.responsible.ResponsibleEntityManager) gerPlugin.getRegisterManager().getEntityManager(ResponsibleEntity.schema);
            ImportMetadata importMetadata = new ImportMetadata();
            importMetadata.setSession(session);
            responsibleEntityManager.parseData(testData, importMetadata);
        } finally {
            session.close();
            testData.close();
        }
    }

    protected void cleanupGladdrregData(SessionManager sessionManager) {
        this.cleanup(sessionManager, new Class[]{
                dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEntity.class,
                dk.magenta.datafordeler.gladdrreg.data.road.RoadEntity.class,
                dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntity.class,
                dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeEntity.class
        });
        QueryManager.clearCaches();
    }

    protected void cleanupPersonData(SessionManager sessionManager) {
        this.cleanup(sessionManager, new Class[] {
                PersonEntity.class,
        });
        QueryManager.clearCaches();
    }
    protected void cleanupCompanyData(SessionManager sessionManager) {
        this.cleanup(sessionManager, new Class[] {
                dk.magenta.datafordeler.cvr.records.CompanyRecord.class,
                dk.magenta.datafordeler.cvr.records.CompanyUnitRecord.class,
                dk.magenta.datafordeler.cvr.records.ParticipantRecord.class,
                dk.magenta.datafordeler.ger.data.company.CompanyEntity.class,
                dk.magenta.datafordeler.ger.data.unit.UnitEntity.class,
                dk.magenta.datafordeler.ger.data.responsible.ResponsibleEntity.class
        });
        QueryManager.clearCaches();
    }

}

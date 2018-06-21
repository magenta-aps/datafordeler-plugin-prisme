package dk.magenta.datafordeler.prisme;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dk.magenta.datafordeler.core.MonitorService;
import dk.magenta.datafordeler.core.arearestriction.AreaRestriction;
import dk.magenta.datafordeler.core.arearestriction.AreaRestrictionType;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.plugin.AreaRestrictionDefinition;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cvr.CvrAreaRestrictionDefinition;
import dk.magenta.datafordeler.cvr.CvrPlugin;
import dk.magenta.datafordeler.cvr.CvrRolesDefinition;
import dk.magenta.datafordeler.cvr.data.company.CompanyRecordQuery;
import dk.magenta.datafordeler.cvr.data.unversioned.Address;
import dk.magenta.datafordeler.cvr.data.unversioned.Municipality;
import dk.magenta.datafordeler.cvr.data.unversioned.PostCode;
import dk.magenta.datafordeler.cvr.records.*;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/prisme/cvr/1")
public class CvrRecordService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private DafoUserManager dafoUserManager;

    @Autowired
    private CvrPlugin cvrPlugin;

    @Autowired
    private MonitorService monitorService;

    private Logger log = LoggerFactory.getLogger(CvrRecordService.class);

    @PostConstruct
    public void init() {
        this.monitorService.addAccessCheckPoint("/prisme/cvr/1/1234");
    }

    @RequestMapping(method = RequestMethod.GET, path = "/{cvrNummer}", produces = {MediaType.APPLICATION_JSON_VALUE})
    public String getSingle(@PathVariable("cvrNummer") String cvrNummer, HttpServletRequest request)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, JsonProcessingException, HttpNotFoundException {

        DafoUserDetails user = dafoUserManager.getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(log, request, user);
        loggerHelper.info(
                "Incoming REST request for PrismeCvrService with cvrNummer " + cvrNummer
        );
        this.checkAndLogAccess(loggerHelper);

        HashSet<String> cvrNumbers = new HashSet<>();
        cvrNumbers.add(cvrNummer);

        CompanyRecordQuery query = new CompanyRecordQuery();
        query.setCvrNumre(cvrNumbers);
        this.applyAreaRestrictionsToQuery(query, user);

        Session session = sessionManager.getSessionFactory().openSession();
        try {
            List<CompanyRecord> records = QueryManager.getAllEntities(session, query, CompanyRecord.class);
            if (!records.isEmpty()) {
                final Session lookupSession = sessionManager.getSessionFactory().openSession();
                try {
                    LookupService service = new LookupService(lookupSession);
                    CompanyRecord companyRecord = records.iterator().next();
                    return objectMapper.writeValueAsString(
                            this.wrapRecord(companyRecord, service)
                    );
                } finally {
                    lookupSession.close();
                }
            }
            throw new HttpNotFoundException("No entity with CVR number " + cvrNummer + " was found");
        } finally {
            session.close();
        }
    }


    private static final String PARAM_UPDATED_SINCE = "updatedSince";
    private static final String PARAM_CVR_NUMBER = "cvrNumber";
    private static final byte[] START_OBJECT = "{".getBytes();
    private static final byte[] END_OBJECT = "}".getBytes();
    private static final byte[] OBJECT_SEPARATOR = ",\n".getBytes();

    @RequestMapping(method = RequestMethod.POST, path = "/", produces = {MediaType.APPLICATION_JSON_VALUE})
    public StreamingResponseBody getBulk(HttpServletRequest request)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException {
        JsonNode requestBody;
        try {
            requestBody = objectMapper.readTree(request.getInputStream());
        } catch (IOException e) {
            throw new InvalidClientInputException(e.getMessage());
        }
        if (!requestBody.isObject()) {
            throw new InvalidClientInputException("Input is not a JSON object");
        }
        ObjectNode requestObject = (ObjectNode) requestBody;

        final OffsetDateTime updatedSince = requestObject.has(PARAM_UPDATED_SINCE) ? Query.parseDateTime(requestObject.get(PARAM_UPDATED_SINCE).asText()) : null;

        final List<String> cvrNumbers = (requestObject.has(PARAM_CVR_NUMBER)) ? this.getCvrNumber(requestObject.get(PARAM_CVR_NUMBER)) : null;


        DafoUserDetails user = dafoUserManager.getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(log, request, user);
        loggerHelper.info(
                "Incoming REST request for PrismeCprService with " +
                        PARAM_UPDATED_SINCE + " = " + updatedSince + " and " +
                        PARAM_CVR_NUMBER + " = " + (cvrNumbers != null && cvrNumbers.size() > 10 ? (cvrNumbers.size() + " cpr numbers") : cvrNumbers)
        );
        this.checkAndLogAccess(loggerHelper);

        HashSet<String> cvr = new HashSet<>();

        if (cvrNumbers != null) {
            for (String cvrNumber : cvrNumbers) {
                try {
                    cvr.add(Integer.toString(Integer.parseInt(cvrNumber, 10)));
                } catch (NumberFormatException e) {
                }
            }
        }
        if (cvr.isEmpty()) {
            throw new InvalidClientInputException("Please specify at least one CVR number");
        }

        CompanyRecordQuery query = new CompanyRecordQuery();
        query.setCvrNumre(cvrNumbers);
        query.setRecordAfter(updatedSince);
        this.applyAreaRestrictionsToQuery(query, user);

        return new StreamingResponseBody() {
            @Override
            public void writeTo(OutputStream outputStream) throws IOException {
                Session session = sessionManager.getSessionFactory().openSession();
                List<CompanyRecord> records = QueryManager.getAllEntities(session, query, CompanyRecord.class);
                LookupService lookupService = new LookupService(session);
                try {
                    boolean first = true;
                    outputStream.write(START_OBJECT);
                    for (CompanyRecord record : records) {
                        if (!first) {
                            outputStream.flush();
                            outputStream.write(OBJECT_SEPARATOR);
                        } else {
                            first = false;
                        }

                        outputStream.write(("\"" + record.getCvrNumber() + "\":").getBytes());
                        outputStream.write(
                                objectMapper.writeValueAsString(
                                        CvrRecordService.this.wrapRecord(record, lookupService)
                                ).getBytes("UTF-8")
                        );
                    }
                    outputStream.write(END_OBJECT);
                    outputStream.flush();
                } finally {
                    session.close();
                }
            }
        };
    }


    protected void checkAndLogAccess(LoggerHelper loggerHelper) throws AccessDeniedException, AccessRequiredException {
        try {
            loggerHelper.getUser().checkHasSystemRole(CvrRolesDefinition.READ_CVR_ROLE);
        }
        catch (AccessDeniedException e) {
            loggerHelper.info("Access denied: " + e.getMessage());
            throw(e);
        }
    }

    private static Pattern nonDigits = Pattern.compile("[^\\d]");
    private List<String> getCvrNumber(JsonNode node) {
        ArrayList<String> cvrNumbers = new ArrayList<>();
        if (node.isArray()) {
            for (JsonNode item : (ArrayNode) node) {
                cvrNumbers.addAll(this.getCvrNumber(item));
            }
        } else if (node.isTextual()) {
            cvrNumbers.add(nonDigits.matcher(node.asText()).replaceAll(""));
        } else if (node.isNumber()) {
            cvrNumbers.add(String.format("%08d", node.asInt()));
        }
        return cvrNumbers;
    }

    private JsonNode wrapRecord(CompanyRecord record, LookupService lookupService) {
        ObjectNode root = objectMapper.createObjectNode();

        root.put("cvrNummer", record.getCvrNumber());

        SecNameRecord nameRecord = this.getLastUpdated(record.getNames(), SecNameRecord.class);
        if (nameRecord != null) {
            root.put("navn", nameRecord.getName());
        }

        CompanyIndustryRecord industryRecord = this.getLastUpdated(record.getPrimaryIndustry(), CompanyIndustryRecord.class);
        if (industryRecord != null) {
            root.put("forretningsområde", industryRecord.getIndustryText());
        }

        ArrayList<CompanyStatusRecord> statusRecords = new ArrayList<>(record.getCompanyStatus());
        if (record.getMetadata() != null) {
            CompanyStatusRecord metaStatusRecord = record.getMetadata().getCompanyStatusRecord(record);
            if (metaStatusRecord != null) {
                statusRecords.add(metaStatusRecord);
            }
        }

        CompanyStatusRecord statusRecord = this.getLastUpdated(statusRecords, CompanyStatusRecord.class);
        if (statusRecord != null) {
            root.put("statuskode", statusRecord.getStatus());
            root.put("statuskodedato", statusRecord.getValidFrom().format(DateTimeFormatter.ISO_LOCAL_DATE));
        }

        AddressRecord addressRecord = this.getLastUpdated(record.getPostalAddress(), AddressRecord.class);

        if (addressRecord == null) {
            addressRecord = this.getLastUpdated(record.getLocationAddress(), AddressRecord.class);
        }
        if (addressRecord != null) {
            Address address = addressRecord.getAddress();
            Municipality municipality = address.getMunicipality();
            int municipalityCode = 0;
            if (municipality != null) {
                municipalityCode = municipality.getCode();
                root.put("myndighedskode", municipalityCode);
                root.put("kommune", municipality.getName());
            }

            int roadCode = address.getRoadCode();
            if (roadCode > 0) {
                root.put("vejkode", roadCode);
                if (municipalityCode > 0 && lookupService != null) {
                    Lookup lookup = lookupService.doLookup(municipalityCode, roadCode);
                    if (lookup.localityCode != 0) {
                        root.put("stedkode", lookup.localityCode);
                    }
                }
            }

            String addressFormatted = address.getAddressFormatted();
            if (addressFormatted != null && !addressFormatted.isEmpty()) {
                root.put("adresse", addressFormatted);
            }
            if (address.getPostBox() > 0) {
                root.put("postboks", address.getPostBox());
            }

            PostCode postCode = address.getPost();
            if (postCode != null) {
                root.put("postnummer", postCode.getPostCode());
                root.put("bynavn", postCode.getPostDistrict());
            } else {
                if (address.getPostnummer() != 0) {
                    root.put("postnummer", address.getPostnummer());
                }
                if (address.getPostdistrikt() != null) {
                    root.put("bynavn", address.getPostdistrikt());
                }
            }
            root.put("landekode", address.getCountryCode());

            String coName = address.getCoName();
            if (coName != null) {
                root.put("co", coName);
            }
        }
        Collection<ContactRecord> emailAddresses = record.getEmailAddress();
        if (emailAddresses != null) {
            ArrayNode emailNode = objectMapper.createArrayNode();
            for (ContactRecord emailAddress : emailAddresses) {
                emailNode.add(emailAddress.getContactInformation());
            }
            root.set("email", emailNode);
        }
        Collection<ContactRecord> phoneNumbers = record.getPhoneNumber();
        if (phoneNumbers != null) {
            ArrayNode phoneNode = objectMapper.createArrayNode();
            for (ContactRecord phoneNumber : phoneNumbers) {
                phoneNode.add(phoneNumber.getContactInformation());
            }
            root.set("telefon", phoneNode);
        }
        Collection<ContactRecord> faxNumbers = record.getFaxNumber();
        if (faxNumbers != null) {
            ArrayNode faxNode = objectMapper.createArrayNode();
            for (ContactRecord faxNumber : faxNumbers) {
                faxNode.add(faxNumber.getContactInformation());
            }
            root.set("telefax", faxNode);
        }
        return root;
    }

    private <T extends CvrBitemporalRecord> T getLastUpdated(Collection<T> records, Class<T> tClass) {
        LocalDate latestEffect = LocalDate.MIN;
        T latest = null;
        for (T record : records) {
            if (record.getValidTo() == null || (latestEffect != null && record.getValidTo().isAfter(latestEffect))) {
                latestEffect = record.getValidTo();
                latest = record;
            }
        }
        return latest;
    }

    protected void applyAreaRestrictionsToQuery(CompanyRecordQuery query, DafoUserDetails user) throws InvalidClientInputException {
        Collection<AreaRestriction> restrictions = user.getAreaRestrictionsForRole(CvrRolesDefinition.READ_CVR_ROLE);
        AreaRestrictionDefinition areaRestrictionDefinition = this.cvrPlugin.getAreaRestrictionDefinition();
        AreaRestrictionType municipalityType = areaRestrictionDefinition.getAreaRestrictionTypeByName(CvrAreaRestrictionDefinition.RESTRICTIONTYPE_KOMMUNEKODER);
        for (AreaRestriction restriction : restrictions) {
            if (restriction.getType() == municipalityType) {
                query.addKommunekodeRestriction(restriction.getValue());
            }
        }
    }

}

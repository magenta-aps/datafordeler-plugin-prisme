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
import dk.magenta.datafordeler.core.util.Bitemporality;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cvr.CvrAreaRestrictionDefinition;
import dk.magenta.datafordeler.cvr.CvrPlugin;
import dk.magenta.datafordeler.cvr.CvrRolesDefinition;
import dk.magenta.datafordeler.cvr.DirectLookup;
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

    @Autowired
    private DirectLookup directLookup;

    private Logger log = LoggerFactory.getLogger(CvrRecordService.class);

    @PostConstruct
    public void init() {
        this.monitorService.addAccessCheckPoint("/prisme/cvr/1/1234");
    }

    @RequestMapping(method = RequestMethod.GET, path = "/{cvrNummer}", produces = {MediaType.APPLICATION_JSON_VALUE})
    public String getSingle(@PathVariable("cvrNummer") String cvrNummer, HttpServletRequest request)
            throws DataFordelerException, JsonProcessingException {

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

        boolean returnParticipantDetails = "1".equals(request.getParameter("returnParticipantDetails"));

        Session session = sessionManager.getSessionFactory().openSession();
        try {
            List<CompanyRecord> records = QueryManager.getAllEntities(session, query, CompanyRecord.class);
            if (!records.isEmpty()) {
                final Session lookupSession = sessionManager.getSessionFactory().openSession();
                try {
                    LookupService service = new LookupService(lookupSession);
                    CompanyRecord companyRecord = records.iterator().next();
                    return objectMapper.writeValueAsString(
                            this.wrapRecord(companyRecord, service, returnParticipantDetails)
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
                                        CvrRecordService.this.wrapRecord(record, lookupService, false)
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

    private JsonNode wrapRecord(CompanyRecord record, LookupService lookupService, boolean returnParticipantDetails) {
        ObjectNode root = objectMapper.createObjectNode();

        root.put("cvrNummer", record.getCvrNumber());

        SecNameRecord nameRecord = this.getLastUpdated(record.getNames());
        if (nameRecord != null) {
            root.put("navn", nameRecord.getName());
        }

        CompanyIndustryRecord industryRecord = this.getLastUpdated(record.getPrimaryIndustry());
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

        CompanyStatusRecord statusRecord = this.getLastUpdated(statusRecords);
        if (statusRecord != null) {
            root.put("statuskode", statusRecord.getStatus());
            root.put("statuskodedato", statusRecord.getValidFrom() != null ? statusRecord.getValidFrom().format(DateTimeFormatter.ISO_LOCAL_DATE) : null);
        }

        AddressRecord addressRecord = this.getLastUpdated(record.getPostalAddress());

        if (addressRecord == null) {
            addressRecord = this.getLastUpdated(record.getLocationAddress());
        }
        if (addressRecord != null) {
            Address address = addressRecord.getAddress();
            AddressMunicipalityRecord municipality = addressRecord.getMunicipality();
            int municipalityCode = 0;
            if (municipality != null) {
                municipalityCode = municipality.getMunicipalityCode();
                root.put("myndighedskode", municipalityCode);
                root.put("kommune", municipality.getMunicipalityName());
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

        ContactRecord emailAddress = this.getLastUpdated(record.getEmailAddress());
        if (emailAddress != null) {
            root.put("email", emailAddress.getContactInformation());
        }
        ContactRecord phoneNumber = this.getLastUpdated(record.getPhoneNumber());
        if (phoneNumber != null) {
            root.put("telefon", phoneNumber.getContactInformation());
        }
        ContactRecord faxNumber = this.getLastUpdated(record.getFaxNumber());
        if (faxNumber != null) {
            root.put("telefax", faxNumber.getContactInformation());
        }

        if (returnParticipantDetails) {
            root.set("deltagere", this.getParticipants(record));
        }

        return root;
    }

    private ArrayNode getParticipants(CompanyRecord record) {
        ArrayNode participantsOutput = objectMapper.createArrayNode();
        OffsetDateTime current = OffsetDateTime.now();
        Bitemporality now = new Bitemporality(current, current, current, current);
        for (CompanyParticipantRelationRecord participant : record.getParticipants()) {
            RelationParticipantRecord relationParticipantRecord = participant.getRelationParticipantRecord();
            if (relationParticipantRecord != null && ("PERSON".equals(relationParticipantRecord.unitType) || "ANDEN_DELTAGER".equals(relationParticipantRecord.unitType))) {
                boolean hasEligibleParticipant = false;

                ObjectNode participantOutput = objectMapper.createObjectNode();
                ArrayNode organizationsOutput = objectMapper.createArrayNode();

                for (OrganizationRecord organization : participant.getOrganizations()) {
                    ArrayNode memberNodes = objectMapper.createArrayNode();
                    boolean found = false;
                    for (OrganizationMemberdataRecord memberdataRecord : organization.getMemberData()) {
                        for (AttributeRecord memberAttribute : memberdataRecord.getAttributes()) {
                            if ("FUNKTION".equals(memberAttribute.getType())) {
                                AttributeValueRecord memberAttributeValue = getLastUpdated(memberAttribute.getValues());
                                if (memberAttributeValue != null && memberAttributeValue.getBitemporality().contains(now)) {
                                    ObjectNode orgMemberNode = objectMapper.createObjectNode();
                                    orgMemberNode.put("funktion", memberAttributeValue.getValue());
                                    memberNodes.add(orgMemberNode);
                                    found = true;
                                }
                            }
                        }
                    }
                    if (found) {
                        hasEligibleParticipant = true;
                        ObjectNode organizationOutput = objectMapper.createObjectNode();
                        organizationsOutput.add(organizationOutput);
                        organizationOutput.put("type", organization.getMainType());
                        for (BaseNameRecord organizationName : organization.getNames()) {
                            String name = organizationName.getName();
                            organizationOutput.put("navn", name);
                        }
                        organizationOutput.set("medlemskaber", memberNodes);
                    }
                }
                if (hasEligibleParticipant) {
                    long unitNumber = relationParticipantRecord.getUnitNumber();
                    participantOutput.put("enhedsNummer", unitNumber);
                    try {
                        ParticipantRecord participantRecord = directLookup.participantLookup(Long.toString(unitNumber, 10));
                        if (participantRecord != null) {
                            Long businessKey = participantRecord.getBusinessKey();
                            if (Objects.equals(businessKey, unitNumber)) {
                                // Foreigner
                            } else {
                                participantOutput.put("deltagerPnr", String.format("%010d", businessKey));
                            }
                        }
                    } catch (Exception e) {
                        log.info(e.getMessage());
                    }
                    participantOutput.set("organisationer", organizationsOutput);
                    participantsOutput.add(participantOutput);
                }
            }
        }
        return participantsOutput;
    }

    private <T extends CvrBitemporalRecord> T getLastUpdated(Collection<T> records) {
        ArrayList<T> list = new ArrayList<>();
        for (T record : records) {
            if (record != null) {
                list.add(record);
            }
        }
        if (list.size() > 1) {
            list.sort(
                    Comparator.comparing(
                            CvrBitemporalRecord::getValidFrom, Comparator.nullsFirst(Comparator.naturalOrder())
                    ).thenComparing(
                            CvrBitemporalRecord::getLastUpdated, Comparator.nullsFirst(Comparator.naturalOrder())
                    )
            );
        }
        return list.isEmpty() ? null : list.get(list.size()-1);
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

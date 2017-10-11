package dk.magenta.datafordeler.prisme;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dk.magenta.datafordeler.core.database.Effect;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cvr.CvrRolesDefinition;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.jws.WebMethod;
import javax.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;
import java.util.List;

@RestController
@RequestMapping("/prisme/cpr/1")
public class CprService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private DafoUserManager dafoUserManager;

    private Logger log = LoggerFactory.getLogger(CprService.class);

    private PersonOutputWrapperPrisme personOutputWrapperPrisme = new PersonOutputWrapperPrisme();

    @WebMethod(exclude = true)
    @RequestMapping(path="/{cprNummer}", produces = {MediaType.APPLICATION_JSON_VALUE})
    public String getSingle(@PathVariable("cprNummer") String cprNummer, HttpServletRequest request)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, JsonProcessingException, HttpNotFoundException {

        DafoUserDetails user = dafoUserManager.getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(log, request, user);
        loggerHelper.info(
                "Incoming REST request for PrismeCprService with cprNummer " + cprNummer
        );
        this.checkAndLogAccess(loggerHelper);

        final Session session = sessionManager.getSessionFactory().openSession();
        try {

            OffsetDateTime now = OffsetDateTime.now();
            session.enableFilter(Registration.FILTER_REGISTRATION_FROM).setParameter(Registration.FILTERPARAM_REGISTRATION_FROM, now);
            session.enableFilter(Registration.FILTER_REGISTRATION_TO).setParameter(Registration.FILTERPARAM_REGISTRATION_TO, now);
            session.enableFilter(Effect.FILTER_EFFECT_FROM).setParameter(Effect.FILTERPARAM_EFFECT_FROM, now);
            session.enableFilter(Effect.FILTER_EFFECT_TO).setParameter(Effect.FILTERPARAM_EFFECT_TO, now);

            LookupService lookupService = new LookupService(session);
            personOutputWrapperPrisme.setLookupService(lookupService);

            PersonQuery personQuery = new PersonQuery();
            personQuery.setPersonnummer(cprNummer);

            List<PersonEntity> personEntities = QueryManager.getAllEntities(session, personQuery, PersonEntity.class);

            if (!personEntities.isEmpty()) {
                PersonEntity personEntity = personEntities.get(0);
                return objectMapper.writeValueAsString(personOutputWrapperPrisme.wrapResult(personEntity));
            }
            throw new HttpNotFoundException("No entity with CPR number "+cprNummer+" was found");
        } finally {
            session.close();
        }
    }

    protected void checkAndLogAccess(LoggerHelper loggerHelper) throws AccessDeniedException, AccessRequiredException {
        try {
            loggerHelper.getUser().checkHasSystemRole(CprRolesDefinition.READ_CPR_ROLE);
        }
        catch (AccessDeniedException e) {
            loggerHelper.info("Access denied: " + e.getMessage());
            throw(e);
        }
    }


}

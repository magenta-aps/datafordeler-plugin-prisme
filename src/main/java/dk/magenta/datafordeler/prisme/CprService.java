package dk.magenta.datafordeler.prisme;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dk.magenta.datafordeler.core.database.Effect;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import org.hibernate.Session;
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

    private PersonOutputWrapperPrisme personOutputWrapperPrisme = new PersonOutputWrapperPrisme();

    @WebMethod(exclude = true)
    @RequestMapping(path="/{cprNummer}", produces = {MediaType.APPLICATION_JSON_VALUE})
    public String getSingle(@PathVariable("cprNummer") String cprNummer, HttpServletRequest request)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, JsonProcessingException, HttpNotFoundException {

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


}

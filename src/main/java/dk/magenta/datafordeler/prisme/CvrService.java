package dk.magenta.datafordeler.prisme;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dk.magenta.datafordeler.core.database.Effect;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.cvr.data.company.CompanyEntity;
import dk.magenta.datafordeler.cvr.data.company.CompanyQuery;
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
@RequestMapping("/prisme/cvr/1")
public class CvrService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    private CompanyOutputWrapperPrisme companyOutputWrapper = new CompanyOutputWrapperPrisme();

    @WebMethod(exclude = true)
    @RequestMapping(path="/{cvrNummer}", produces = {MediaType.APPLICATION_JSON_VALUE})
    public String getSingle(@PathVariable("cvrNummer") String cvrNummer, HttpServletRequest request)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, JsonProcessingException, HttpNotFoundException {

        final Session session = sessionManager.getSessionFactory().openSession();
        try {

            OffsetDateTime now = OffsetDateTime.now();
            session.enableFilter(Registration.FILTER_REGISTRATION_FROM).setParameter(Registration.FILTERPARAM_REGISTRATION_FROM, now);
            session.enableFilter(Registration.FILTER_REGISTRATION_TO).setParameter(Registration.FILTERPARAM_REGISTRATION_TO, now);
            session.enableFilter(Effect.FILTER_EFFECT_FROM).setParameter(Effect.FILTERPARAM_EFFECT_FROM, now);
            session.enableFilter(Effect.FILTER_EFFECT_TO).setParameter(Effect.FILTERPARAM_EFFECT_TO, now);

            LookupService lookupService = new LookupService(session);
            companyOutputWrapper.setLookupService(lookupService);

            CompanyQuery companyQuery = new CompanyQuery();
            companyQuery.setCVRNummer(cvrNummer);

            List<CompanyEntity> companyEntities = QueryManager.getAllEntities(session, companyQuery, CompanyEntity.class);

            if (!companyEntities.isEmpty()) {
                CompanyEntity companyEntity = companyEntities.get(0);
                return objectMapper.writeValueAsString(companyOutputWrapper.wrapResult(companyEntity));
            }
            throw new HttpNotFoundException("No entity with CVR number "+cvrNummer+" was found");
        } finally {
            session.close();
        }
    }


}

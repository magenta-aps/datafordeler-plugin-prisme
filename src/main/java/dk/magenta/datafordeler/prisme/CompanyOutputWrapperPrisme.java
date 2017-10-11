package dk.magenta.datafordeler.prisme;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dk.magenta.datafordeler.core.database.Effect;
import dk.magenta.datafordeler.core.fapi.OutputWrapper;
import dk.magenta.datafordeler.cvr.data.company.CompanyBaseData;
import dk.magenta.datafordeler.cvr.data.company.CompanyEffect;
import dk.magenta.datafordeler.cvr.data.company.CompanyEntity;
import dk.magenta.datafordeler.cvr.data.company.CompanyRegistration;
import dk.magenta.datafordeler.cvr.data.unversioned.Address;
import dk.magenta.datafordeler.cvr.data.unversioned.Industry;
import dk.magenta.datafordeler.cvr.data.unversioned.Municipality;
import dk.magenta.datafordeler.cvr.data.unversioned.PostCode;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;

public class CompanyOutputWrapperPrisme extends OutputWrapper<CompanyEntity> {

    private ObjectMapper objectMapper;

    private LookupService lookupService;

    public void setLookupService(LookupService lookupService) {
        this.lookupService = lookupService;
    }

    @Override
    public Object wrapResult(CompanyEntity input) {

        objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("cvrNummer", input.getCvrNumber());

        // Registrations
        for (CompanyRegistration companyRegistration : input.getRegistrations()) {
            for (CompanyEffect virkning : companyRegistration.getEffects()) {
                for (CompanyBaseData companyBaseData : virkning.getDataItems()) {
                    this.wrapDataObject(root, companyBaseData, this.lookupService);
                }
            }
        }
        return root;
    }

    protected ObjectNode wrapDataObject(ObjectNode output, CompanyBaseData dataItem, LookupService lookupService) {
        NodeWrapper wrapper = new NodeWrapper(output);

        wrapper.put("virksomhedsnavn", dataItem.getCompanyName());

        Industry industry = dataItem.getPrimaryIndustry();
        if (industry != null) {
            // wrapper.put("brancheKode", industry.getIndustryCode());
            wrapper.put("brancheNavn", industry.getIndustryText());
        }

        String statusCode = dataItem.getStatusCode();
        wrapper.put("statusKode", statusCode);
        if (statusCode != null) {
            wrapper.put("statusKodeOpdateret", this.getLastEffectTimeFormatted(dataItem.getEffects()));
        }

        Address address = dataItem.getPostalAddress();
        if (address == null) {
            address = dataItem.getLocationAddress();
        }
        if (address != null) {
            int municipalityCode = 0;
            int roadCode = address.getRoadCode();
            Municipality municipality = address.getMunicipality();
            if (municipality != null) {
                wrapper.put("kommuneKode", municipality.getCode());
                wrapper.put("kommuneNavn", municipality.getName());
                municipalityCode = municipality.getCode();
            }
            wrapper.put("vejKode", roadCode);
            if (municipalityCode > 0 && roadCode > 0 && this.lookupService != null) {
                Lookup lookup = lookupService.doLookup(municipalityCode, roadCode);
                if (lookup.localityCode != 0) {
                    wrapper.put("stedKode", lookup.localityCode);
                }
            }
            wrapper.put("adresse", address.getAddressFormatted());
            if (address.getPostBox() > 0) {
                wrapper.put("postboks", address.getPostBox());
            }

            PostCode postCode = address.getPost();
            if (postCode != null) {
                wrapper.put("postNummer", postCode.getPostCode());
                wrapper.put("postDistrikt", postCode.getPostDistrict());
            }

            wrapper.put("landeKode", address.getCountryCode());
        }

        return wrapper.getNode();
    }

    private OffsetDateTime getLastEffectTime(Collection<? extends Effect> effects) {
        OffsetDateTime latest = OffsetDateTime.MIN;
        for (Effect effect : effects) {
            OffsetDateTime start = effect.getEffectFrom();
            if (start.isAfter(latest)) {
                latest = start;
            }
        }
        return latest;
    }

    private String getLastEffectTimeFormatted(Collection<? extends Effect> effects) {
        OffsetDateTime statusKodeUpdated = this.getLastEffectTime(effects);
        if (statusKodeUpdated.isAfter(OffsetDateTime.MIN)) {
            return statusKodeUpdated.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        return null;
    }

}

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
import java.util.List;

public class CompanyOutputWrapperPrisme extends OutputWrapper<CompanyEntity> {

    private ObjectMapper objectMapper;

    private LookupService lookupService;

    public void setLookupService(LookupService lookupService) {
        this.lookupService = lookupService;
    }

    @Override
    public ObjectNode wrapResult(CompanyEntity input) {

        objectMapper = new ObjectMapper();

        // Root
        NodeWrapper root = new NodeWrapper(objectMapper.createObjectNode());
        root.put("cvrNummer", input.getCvrNumber());

        OffsetDateTime highestStatusTime = OffsetDateTime.MIN;
        // Registrations
        for (CompanyRegistration companyRegistration : input.getRegistrations()) {
            for (CompanyEffect virkning : companyRegistration.getEffects()) {
                OffsetDateTime effectFrom = virkning.getEffectFrom();
                List<CompanyBaseData> dataItems = virkning.getDataItems();
                for (CompanyBaseData companyBaseData : dataItems) {
                    this.wrapDataObject(root, companyBaseData);
                }
                if (effectFrom != null) {
                    if (effectFrom.isAfter(highestStatusTime)) {
                        for (CompanyBaseData companyBaseData : dataItems) {
                            if (companyBaseData.getStatusCode() != null) {
                                highestStatusTime = effectFrom;
                            }
                        }
                    }
                }
            }
        }
        if (highestStatusTime.isAfter(OffsetDateTime.MIN)) {
            root.put("statuskodedato", highestStatusTime.format(DateTimeFormatter.ISO_LOCAL_DATE));
        }
        return root.getNode();
    }

    protected void wrapDataObject(NodeWrapper output, CompanyBaseData dataItem) {

        output.put("navn", dataItem.getCompanyName());

        Industry industry = dataItem.getPrimaryIndustry();
        if (industry != null) {
            // output.put("brancheKode", industry.getIndustryCode());
            output.put("forretningsområde", industry.getIndustryText());
        }

        String statusCode = dataItem.getStatusCode();
        output.put("statuskode", statusCode);
        // if (statusCode != null) {
        //     output.put("statuskodedato", this.getLastEffectTimeFormatted(dataItem.getEffects()));
        // }

        Address address = dataItem.getPostalAddress();
        if (address == null) {
            address = dataItem.getLocationAddress();
        }
        if (address != null) {
            int municipalityCode = 0;
            int roadCode = address.getRoadCode();
            Municipality municipality = address.getMunicipality();
            if (municipality != null) {
                output.put("myndighedskode", municipality.getCode());
                output.put("kommune", municipality.getName());
                municipalityCode = municipality.getCode();
            }
            output.put("vejkode", roadCode);
            if (municipalityCode > 0 && roadCode > 0 && this.lookupService != null) {
                Lookup lookup = lookupService.doLookup(municipalityCode, roadCode);
                if (lookup.localityCode != 0) {
                    output.put("stedkode", lookup.localityCode);
                }
            }
            output.put("adresse", address.getAddressFormatted());
            if (address.getPostBox() > 0) {
                output.put("postboks", address.getPostBox());
            }

            PostCode postCode = address.getPost();
            if (postCode != null) {
                output.put("postnummer", postCode.getPostCode());
                output.put("bynavn", postCode.getPostDistrict());
            }

            output.put("landekode", address.getCountryCode());
        }

    }

}

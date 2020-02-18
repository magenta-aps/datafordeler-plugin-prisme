package dk.magenta.datafordeler.prisme;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import dk.magenta.datafordeler.core.fapi.BaseQuery;
import dk.magenta.datafordeler.core.fapi.OutputWrapper;
import dk.magenta.datafordeler.core.util.BitemporalityComparator;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.records.CprBitemporalRecord;
import dk.magenta.datafordeler.cpr.records.CprBitemporality;
import dk.magenta.datafordeler.cpr.records.person.data.*;
import dk.magenta.datafordeler.geo.GeoLookupDTO;
import dk.magenta.datafordeler.geo.GeoLookupService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Comparator.naturalOrder;

/**
 * Outputwrapper for serialicing a adresssequence
 */
@Component
public class PersonAdressHistoryOutputWrapperPrisme extends OutputWrapper {

    private Logger log = LogManager.getLogger(PersonAdressHistoryOutputWrapperPrisme.class.getCanonicalName());

    @Autowired
    private ObjectMapper objectMapper;

    private GeoLookupService lookupService;

    public void setLookupService(GeoLookupService lookupService) {
        this.lookupService = lookupService;
    }

    public static CprBitemporality getBitemporality(CprBitemporalRecord record) {
        return record.getBitemporality();
    }


    private static Comparator bitemporalComparator2 = Comparator.comparing(PersonAdressHistoryOutputWrapperPrisme::getBitemporality, BitemporalityComparator.ALL)
            .thenComparing(CprBitemporalRecord::getEffectFrom, Comparator.nullsLast(naturalOrder()));


    public Object wrapRecordResult(PersonEntity input, BaseQuery query) {

        // Root
        NodeWrapper root = new NodeWrapper(objectMapper.createObjectNode());
        root.put("cprNummer", input.getPersonnummer());
        NameDataRecord nameData = FilterUtilities.findNewestUnclosed(input.getName());

        if (nameData != null) {
            StringJoiner nameJoiner = new StringJoiner(" ");
            if (!nameData.getFirstNames().isEmpty()) {
                nameJoiner.add(nameData.getFirstNames());
            }
            if (!nameData.getMiddleName().isEmpty()) {
                nameJoiner.add(nameData.getMiddleName());
            }
            if (nameJoiner.length() > 0) {
                root.put("fornavn", nameJoiner.toString());
            }
            if (nameData.getLastName() != null && !nameData.getLastName().isEmpty()) {
                root.put("efternavn", nameData.getLastName());
            }
        }

        PersonStatusDataRecord personStatusData = FilterUtilities.findNewestUnclosed(input.getStatus());
        if (personStatusData != null) {
            root.put("statuskode", personStatusData.getStatus());
            root.put("statuskodedato", PersonOutputWrapperPrisme.formatDate(
                    personStatusData.getEffectFrom() != null ? personStatusData.getEffectFrom() : personStatusData.getRegistrationFrom()
            ));
        }

        CivilStatusDataRecord civilStatusData = FilterUtilities.findNewestUnclosed(input.getCivilstatus());
        if (civilStatusData != null) {
            root.put("civilstand", civilStatusData.getCivilStatus());
        }

        Set<AddressDataRecord> personAddressDataList = input.getAddress();

        ObjectMapper mapper = new ObjectMapper();
        List<PersonAdressItem> personAdressItemList = new ArrayList<PersonAdressItem>();


        AtomicReference<OffsetDateTime> org = new AtomicReference<OffsetDateTime>();
        AtomicReference<adressSequenceProgress> progress = new AtomicReference<adressSequenceProgress>();
        progress.set(adressSequenceProgress.INITIAL);

        personAddressDataList.stream().filter(r -> r.getBitemporality().registrationTo == null).sorted(bitemporalComparator2).forEach(
                personAddressData -> {
                    AddressDataRecord adressRecord = (AddressDataRecord) personAddressData;
                    PersonAdressItem personAdress = new PersonAdressItem();
                    if (progress.get().equals(adressSequenceProgress.INITIAL) && org.get() == null) {
                        progress.set(adressSequenceProgress.FIRSTFOUND);
                    } else if ((progress.get().equals(adressSequenceProgress.FIRSTFOUND) || progress.get().equals(adressSequenceProgress.NEWFOUND)) && org.get().equals(adressRecord.getEffectFrom())) {
                        if(org.get().equals(adressRecord.getEffectFrom())) {
                            progress.set(adressSequenceProgress.NEWFOUND);
                        } else if(adressRecord.getEffectTo() == null) {
                            progress.set(adressSequenceProgress.LASTFOUND);
                        }
                    } else {
                        //If this flag i s raised is means that the person has an unusual adresssequense, not necessarily wrong.
                        log.error("ADRESSES IS NOT SEQUENTIAL: " + input.getPersonnummer());
                    }
                    if(adressRecord.getEffectTo() != null) {
                        org.set(adressRecord.getEffectTo());
                    }

                    personAdress.setMyndighedskode(adressRecord.getMunicipalityCode());
                    personAdress.setVejkode(adressRecord.getRoadCode());
                    GeoLookupDTO lookupDto = lookupService.doLookup(adressRecord.getMunicipalityCode(), adressRecord.getRoadCode(), adressRecord.getHouseNumber());
                    personAdress.setKommune(lookupDto.getMunicipalityName());
                    personAdress.setKommune(lookupDto.getRoadName());
                    personAdress.setPostnummer(lookupDto.getPostalCode());
                    personAdress.setBynavn(lookupDto.getPostalDistrict());
                    personAdress.setStedkode(lookupDto.getLocalityCodeNumber());
                    personAdress.setKommune(lookupDto.getMunicipalityName());

                    String buildingNumber = lookupDto.getbNumber();
                    String roadName = lookupDto.getRoadName();

                    if (roadName != null) {
                        personAdress.setAdresse(PersonOutputWrapperPrisme.getAddressFormatted(
                                roadName,
                                adressRecord.getHouseNumber(),
                                null,
                                null, null,
                                adressRecord.getFloor(),
                                adressRecord.getDoor(),
                                buildingNumber
                        ));
                    } else if (buildingNumber != null && !buildingNumber.isEmpty()) {
                        personAdress.setAdresse(buildingNumber);
                    }

                    if (adressRecord.getMunicipalityCode() > 0 && adressRecord.getMunicipalityCode() < 900) {
                        personAdress.setLandekode("DK");
                    } else if (adressRecord.getMunicipalityCode() > 900) {
                        personAdress.setLandekode("GL");
                    }

                    personAdress.setStartdato(PersonOutputWrapperPrisme.formatDate(adressRecord.getEffectFrom()));
                    personAdress.setSlutdato(PersonOutputWrapperPrisme.formatDate(adressRecord.getEffectTo()));
                    personAdressItemList.add(personAdress);
                }
        );

        ArrayNode jsonAdressArray = mapper.valueToTree(personAdressItemList);
        root.putArray("addresses", jsonAdressArray);

        return root.getNode();
    }

    /**
     * Enumerations used for validating the secuences of adresses on a person.
     */
    public enum adressSequenceProgress {
        INITIAL,
        FIRSTFOUND,
        NEWFOUND,
        LASTFOUND
    }
}
package dk.magenta.datafordeler.prisme;

import dk.magenta.datafordeler.core.database.Identification;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityData;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEffect;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityData;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEffect;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityQuery;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeData;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeEffect;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeEntity;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadData;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadEffect;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadEntity;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadQuery;
import org.hibernate.Session;

import java.time.OffsetDateTime;
import java.util.List;

public class LookupService {

    private Session session;

    public LookupService(Session session) {
        this.session = session;
    }

    public Lookup doLookup(int municipalityCode, int roadCode) {
        Lookup lookup = new Lookup();
        OffsetDateTime now = OffsetDateTime.now();

        MunicipalityEntity municipalityEntity = this.getMunicipality(session, municipalityCode);
        if (municipalityEntity != null) {
            for (MunicipalityEffect municipalityEffect : municipalityEntity.getRegistrationAt(now).getEffectsAt(now)) {
                for (MunicipalityData municipalityData : municipalityEffect.getDataItems()) {
                    if (municipalityData.getName() != null) {
                        lookup.municipalityName = municipalityData.getName();
                        break;
                    }
                }
            }

            RoadEntity roadEntity = this.getRoad(session, municipalityEntity, roadCode);
            if (roadEntity != null) {
                for (RoadEffect roadEffect : roadEntity.getRegistrationAt(now).getEffectsAt(now)) {
                    for (RoadData roadData : roadEffect.getDataItems()) {
                        if (roadData.getName() != null) {
                            lookup.roadName = roadData.getName();
                            break;
                        }
                    }
                }


                LocalityEntity localityEntity = this.getLocality(session, roadEntity);
                if (localityEntity != null) {
                    for (LocalityEffect localityEffect : localityEntity.getRegistrationAt(now).getEffectsAt(now)) {
                        for (LocalityData localityData : localityEffect.getDataItems()) {
                            if (localityData.getCode() > 0) {
                                lookup.localityCode = localityData.getCode();
                                break;
                            }
                        }
                    }


                    PostalCodeEntity postalCodeEntity = this.getPostalCode(session, localityEntity);
                    if (postalCodeEntity != null) {
                        for (PostalCodeEffect postalCodeEffect : postalCodeEntity.getRegistrationAt(now).getEffectsAt(now)) {
                            for (PostalCodeData postalCodeData : postalCodeEffect.getDataItems()) {
                                if (postalCodeData.getCode() > 0) {
                                    lookup.postalCode = postalCodeData.getCode();
                                    lookup.postalDistrict = postalCodeData.getName();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        return lookup;
    }



    MunicipalityEntity getMunicipality(Session session, int municipalityCode) {
        try {
            OffsetDateTime now = OffsetDateTime.now();
            MunicipalityQuery municipalityQuery = new MunicipalityQuery();
            municipalityQuery.setCode(Integer.toString(municipalityCode));
            List<MunicipalityEntity> municipalityEntities = QueryManager.getAllEntities(session, municipalityQuery, MunicipalityEntity.class);
            return municipalityEntities.get(0);
        } catch (IndexOutOfBoundsException e) {
        }
        return null;
    }

    RoadEntity getRoad(Session session, MunicipalityEntity municipalityEntity, int roadCode) {
        try {
            RoadQuery roadQuery = new RoadQuery();
            roadQuery.setCode(Integer.toString(roadCode));
            roadQuery.setMunicipalityIdentifier(municipalityEntity.getUUID().toString());
            List<RoadEntity> roadEntities = QueryManager.getAllEntities(session, roadQuery, RoadEntity.class);
            return roadEntities.get(0);
        } catch (IndexOutOfBoundsException | NullPointerException e) {
        }
        return null;
    }

    private LocalityEntity getLocality(Session session, RoadEntity roadEntity) {
        OffsetDateTime now = OffsetDateTime.now();
        for (RoadEffect roadEffect : roadEntity.getRegistrationAt(now).getEffectsAt(now)) {
            for (RoadData roadData : roadEffect.getDataItems()) {
                Identification locationIdentification = roadData.getLocation();
                if (locationIdentification != null) {
                    LocalityEntity locality = QueryManager.getEntity(session, locationIdentification, LocalityEntity.class);
                    if (locality != null) {
                        return locality;
                    }
                }
            }
        }
        return null;
    }

    private PostalCodeEntity getPostalCode(Session session, LocalityEntity localityEntity) {
        OffsetDateTime now = OffsetDateTime.now();
        for (LocalityEffect localityEffect : localityEntity.getRegistrationAt(now).getEffectsAt(now)) {
            for (LocalityData localityData : localityEffect.getDataItems()) {
                Identification postalCodeIdentification = localityData.getPostalCode();
                if (postalCodeIdentification != null) {
                    PostalCodeEntity postalCode = QueryManager.getEntity(session, postalCodeIdentification, PostalCodeEntity.class);
                    if (postalCode != null) {
                        return postalCode;
                    }
                }
            }
        }
        return null;
    }

}

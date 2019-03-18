package com.trivago.mp.casestudy;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Your task will be to implement two functions, one for loading the data which is stored as .csv files in the ./data
 * folder and one for performing the actual search.
 * <p>
 *
 * Inspired by Author : https://github.com/admitriev/java-case-study
 */
public class HotelSearchEngineImpl implements HotelSearchEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(HotelSearchEngineImpl.class);

    private Map<Integer, String> cities;
    private Map<Integer, Advertiser> advertisers;
    private Map<Integer, Set<Integer>> hotelsPerAdvertiser;
    private Map<Integer, Hotel> hotels;
    private Map<String, Set<Integer>> hotelsPerCity = new HashMap<>();

    @Override
    public void initialize() {
        // All the relevant data viz. Cities, Hotels, Advertisers, HotelsPerCity, HotelsPerAdvertiser has been read and stored as Map
        // Ideally the data should be persisted under Database and ORM shall be used to map database rows to POJOs
        // Here our database is CSV file and reading the CSV file for each row as and when is inefficient. Hence all rows are read at once and stored as Map data structure
        cities = CSVDataLoader.getCities();
        advertisers = CSVDataLoader.getAdvertisers();
        hotelsPerAdvertiser = CSVDataLoader.mapAdvertiserToHotel();
        hotels = CSVDataLoader.getHotels(hotelsPerCity, cities);
        LOGGER.info("Initialised with all CSV data files : {} cities, {} hotels, {} advertisers", cities.size(), hotels.size(), advertisers.size());
    }

    @Override
    public List<HotelWithOffers> performSearch(String cityName, DateRange dateRange, OfferProvider offerProvider) {

        if (cities.keySet().stream().noneMatch(cityId -> cityName.equalsIgnoreCase(cities.get(cityId)))) {
            LOGGER.info("Sorry! We don't have any Partner Hotels for the input city : {}", cityName);
            return Collections.emptyList();
        }

        Set<Integer> setOfHotelIdsForInputCity = hotelsPerCity.getOrDefault(cityName, Collections.emptySet());
        if (setOfHotelIdsForInputCity.isEmpty()) {
            LOGGER.info("Sorry! No Hotels are available for the Input City {}", cityName);
            return Collections.emptyList();
        }

        if (dateRange.getStartDate() >= dateRange.getEndDate()) {
            LOGGER.info("Start date {} cannot be greater or equal to End date {}", dateRange.getStartDate(), dateRange.getEndDate());
            return Collections.emptyList();
        }

        LOGGER.debug("Preforming search for {} hotels in {} city", setOfHotelIdsForInputCity, cityName);
        final Map<Integer, List<Offer>> offersByHotelId = requestOffersFromAdvertiser(dateRange, offerProvider, setOfHotelIdsForInputCity);
        // HotelWithOffers a represents a Hotel and a list of concrete Advertisers for that Hotel
        // e.g. For input city Munich - "hotel_central_inn_Munich" with hotelId=129 has a list of Offers by many Advertisers viz. "travel_booking_worldwide", "beach_expert_foryou", "beach_expert.de" and so on
        List<HotelWithOffers> resultHotelWithOffers = offersByHotelId.keySet().stream().map(hotelIdAsKey -> {
            HotelWithOffers hotelWithOffers = new HotelWithOffers(hotels.get(hotelIdAsKey));
            hotelWithOffers.setOffers(offersByHotelId.get(hotelIdAsKey));
            return hotelWithOffers;
        }).collect(Collectors.toList());
        LOGGER.debug("Found {} offers for hotels in {} city", resultHotelWithOffers.size(), cityName);
        return resultHotelWithOffers;
    }

    /**
     * Calls offerProvider.getOffersFromAdvertiser() and returns a map of offers with HotelIds for input city as Key and List of Offer as Value
     * @param dateRange
     * @param offerProvider
     * @param hotelIdsForInputCity
     * @return
     */
    private Map<Integer, List<Offer>> requestOffersFromAdvertiser(DateRange dateRange, OfferProvider offerProvider, Set<Integer> hotelIdsForInputCity) {
        Map<Integer, List<Offer>> offersByHotelId = new HashMap<>();

        hotelsPerAdvertiser.keySet().stream().forEach(advertiserId -> {

            Set<Integer> hotelIdsPerAdvertiser = hotelsPerAdvertiser.get(advertiserId);
            // the common hotelIds among the two Sets hotelIdsPerAdvertiser and hotelIdsForInputCity should be the output Offers per Advertiser
            // For example for Munich city : hotelIdsForInputCity - 89, 90, 91, 92, 93 and so on. AdvertiserId 42 has an offer for HotelId 89

            List<Integer> hotelIdsInCityPerAdvertiser = hotelIdsPerAdvertiser
                    .stream()
                    .filter(hotelIdsForInputCity::contains) // set of hotelIds present in a city shall contain the hotelId offered by the Advertiser
                    .collect(Collectors.toList());

            // Skip the call to getOffersFromAdvertiser() (it is a costly operation) if the advertiser doesn't have any matching hotel for the input city
            // Frequent calls to the same city can be Cached.
            if (!hotelIdsInCityPerAdvertiser.isEmpty()) {
                LOGGER.debug("Requesting offers from advertiser {}", advertiserId);
                Map<Integer, Offer> mapOfOffersFromAdvertiser = offerProvider.getOffersFromAdvertiser(
                        advertisers.get(advertiserId),
                        hotelIdsInCityPerAdvertiser,
                        dateRange);

                LOGGER.debug("Advertiser {} has offered {} hotels", advertiserId, mapOfOffersFromAdvertiser.size());
                mapOfOffersFromAdvertiser.forEach((hotelIdAsKey, offerPerAdvertiserAsValue) ->
                        offersByHotelId.computeIfAbsent(hotelIdAsKey, listOfOfferPerAdvertiser -> new ArrayList<>()).add(offerPerAdvertiserAsValue));
            }
        });
        return offersByHotelId;
    }
}

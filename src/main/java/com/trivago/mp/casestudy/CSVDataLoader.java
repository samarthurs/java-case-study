package com.trivago.mp.casestudy;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;

public class CSVDataLoader {

    /**
     * Reads the CSV data file and returns a Stream of CSVRecord
     *
     * @param csvDatafile
     * @return
     */
    private static Stream<CSVRecord> getCSVParserStream(String csvDatafile) {
        // csv files are present under the base directory /data as /data/advertisers.csv, /data/cities.csv and so on...
        try (CSVParser parser = new CSVParser(Files.newBufferedReader(Paths.get("data/" + csvDatafile)), CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withIgnoreHeaderCase()
                .withTrim())) {
            return parser.getRecords().stream();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads cities.csv file and creates a Map with CityId as Key and CityName as Value
     *
     * @return
     * @throws IOException
     */
    public static Map<Integer, String> getCities() {
        Stream<CSVRecord> cityCsvRecordStream = getCSVParserStream("cities.csv");
        return cityCsvRecordStream.collect(Collectors.toMap(cityCsvRecord -> parseInt(cityCsvRecord.get("id")), cityCsvRecord -> cityCsvRecord.get("city_name")));
    }

    /**
     * Reads advertisers.csv and create a Map with AdvertiserId as Key and Advertiser object as Value
     *
     * @return
     */
    public static Map<Integer, Advertiser> getAdvertisers() {
        Stream<CSVRecord> advertiserStream = getCSVParserStream("advertisers.csv");
        return advertiserStream.collect(Collectors.toMap(advCsvRec -> parseInt(advCsvRec.get("id")),
                advCsvRec -> new Advertiser(parseInt(advCsvRec.get("id")), advCsvRec.get("advertiser_name")))); // AdvertiserId as Key and Advertiser object as Value
    }

    /**
     * Create a Map of advertiser to hotel with advertiserId as Key and setOfHotels per advertiserId as Value
     *
     * @return
     */
    public static Map<Integer, Set<Integer>> mapAdvertiserToHotel() {

        // Map of hotels available per Advertiser
        Map<Integer, Set<Integer>> hotelsByAdvertiser = new HashMap<>();
        Stream<CSVRecord> hotelAdvertiserStream = getCSVParserStream("hotel_advertiser.csv");

        // M Advertisers can have N Hotels
        // Advertiser 1 has 29 hotels across different cities 1 -- {45, 66, 111 ... 526, 533, 543}
        hotelAdvertiserStream.forEach(hotelAdvertiserCsvRecord -> {
            int advertiserId = parseInt(hotelAdvertiserCsvRecord.get("advertiser_id"));
            int hotelIdAdvertiserHasToOffer = parseInt(hotelAdvertiserCsvRecord.get("hotel_id"));
            hotelsByAdvertiser.computeIfAbsent(advertiserId, setOfHotelIdsPerAdvertiser -> new HashSet<>()).add(hotelIdAdvertiserHasToOffer);
        });

        return hotelsByAdvertiser;
    }

    /**
     * Create a map of Hotels with HotelId as Key and Hotel object as Value
     * Updates the map of hotels by City with CityName as Key and Set of Hotels(Hotels Ids) as Value
     *
     * @param hotelsPerCity
     * @param cityById
     * @return
     */
    public static Map<Integer, Hotel> getHotels(Map<String, Set<Integer>> hotelsPerCity,
                                                Map<Integer, String> cityById) {
        Map<Integer, Hotel> result = new HashMap<>();
        Stream<CSVRecord> hotelStream = getCSVParserStream("hotels.csv");

        hotelStream.forEach(hotelCsvRecord -> {
            Hotel hotel = csvRecordToHotel(hotelCsvRecord);
            result.put(hotel.getId(), hotel);

            int cityId = parseInt(hotelCsvRecord.get("city_id")); // city_id is available as column under hotels.csv file
            String cityName = cityById.get(cityId); // cityById map has cityId as Key and cityName as Value

            // hotelsByCity map contains a set of hotels(hotelsId) available in a city. For example Munich has 87 hotels in the given hotels.csv file
            // Munich : {89, 90, 91 ... 173, 174 , 175}
            hotelsPerCity.computeIfAbsent(cityName, setOfHotelIdsPerCity -> new HashSet<>()).add(hotel.getId());

        });
        return result;
    }

    /**
     * Create a Hotel object for each row in the hotels.csv file
     *
     * @param record
     * @return
     */
    private static Hotel csvRecordToHotel(CSVRecord record) {
        return new Hotel(
                parseInt(record.get("id")),
                record.get("name"),
                parseInt(record.get("rating")),
                parseInt(record.get("stars")));
    }

    public static void main(String[] args) {

        Map<Integer, String> cityMap;
        cityMap = CSVDataLoader.getCities();
        for (Map.Entry<Integer, String> cityEntry : cityMap.entrySet()) {
            System.out.println(cityEntry.getKey() + " --> " + cityEntry.getValue());
        }

        Map<String, Set<Integer>> hotelsByCity = new HashMap<>();
        Map<Integer, Hotel> hotelMap = getHotels(hotelsByCity, cityMap);

        System.out.println("Hotels in Paris : " + hotelsByCity.get("Paris"));

    }

}


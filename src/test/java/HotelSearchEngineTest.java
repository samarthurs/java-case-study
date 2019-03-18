import com.trivago.mp.casestudy.*;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertFalse;

public class HotelSearchEngineTest {
    final Random random = new Random(42);

    private OfferProvider dummyOfferProvider = (advertiser, hotelIds, dateRange) -> {
        Map<Integer, Offer> result = new HashMap<>();
        for (Integer hotelId : hotelIds) {
            if (random.nextDouble() > 0.25) {
                result.put(hotelId, new Offer(advertiser, random.nextInt(100) + 50, random.nextInt(10)));
            }
        }
        return result;
    };

    @Test
    public void testNoException() {
        HotelSearchEngine hotelSearchEngine = new HotelSearchEngineImpl();

        try {
            hotelSearchEngine.initialize();
            hotelSearchEngine.performSearch("Berlin", new DateRange(20180214, 20180216), dummyOfferProvider);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testEmptyResultsReturned() {
        HotelSearchEngine hotelSearchEngine = new HotelSearchEngineImpl();

        hotelSearchEngine.initialize();
        List<HotelWithOffers> hotelsWithOffers = hotelSearchEngine.performSearch("Zurich",
                                                                                 new DateRange(20180214, 20180216),
                                                                                 dummyOfferProvider
        );
        assertTrue("Hotels result for Zurich is Empty", hotelsWithOffers.isEmpty());
    }

    @Test
    public void testInvalidCity() {
        assertTrue("No results for city Zurich", CSVDataLoader.getCities().entrySet().stream().noneMatch(entry -> "Zurich".equalsIgnoreCase(entry.getValue())));
    }

    @Test
    public void testInvalidDateRange(){
        HotelSearchEngine hotelSearchEngine = new HotelSearchEngineImpl();
        hotelSearchEngine.initialize();
        List<HotelWithOffers> hotelsWithOffers = hotelSearchEngine.performSearch("Lissabon",
                new DateRange(20180216, 20180216),
                dummyOfferProvider);

        assertTrue("Invalid DateRange. Thus, the returned list is empty", hotelsWithOffers.isEmpty());
    }

    @Test
    public void testResultsReturned() {
        HotelSearchEngine hotelSearchEngine = new HotelSearchEngineImpl();

        hotelSearchEngine.initialize();
        List<HotelWithOffers> hotelsWithOffers = hotelSearchEngine.performSearch("Berlin",
                new DateRange(20180214, 20180216),
                dummyOfferProvider
        );
        assertFalse("the result is not empty", hotelsWithOffers.isEmpty());
    }


    @Test
    public void testCorrectHotelsReturned() {
        HotelSearchEngine hotelSearchEngine = new HotelSearchEngineImpl();

        hotelSearchEngine.initialize();
        List<HotelWithOffers> hotelsWithOffers = hotelSearchEngine.performSearch("Munich",
                                                                                 new DateRange(20180214, 20180216),
                                                                                 dummyOfferProvider
        );

        @SuppressWarnings("serial") Set<Integer> hotelsInMunich = new HashSet<Integer>() {{
            addAll(Arrays.asList(89,
                                 90,
                                 91,
                                 92,
                                 93,
                                 94,
                                 95,
                                 96,
                                 97,
                                 98,
                                 99,
                                 100,
                                 101,
                                 102,
                                 103,
                                 104,
                                 105,
                                 106,
                                 107,
                                 108,
                                 109,
                                 110,
                                 111,
                                 112,
                                 113,
                                 114,
                                 115,
                                 116,
                                 117,
                                 118,
                                 119,
                                 120,
                                 121,
                                 122,
                                 123,
                                 124,
                                 125,
                                 126,
                                 127,
                                 128,
                                 129,
                                 130,
                                 131,
                                 132,
                                 133,
                                 134,
                                 135,
                                 136,
                                 137,
                                 138,
                                 139,
                                 140,
                                 141,
                                 142,
                                 143,
                                 144,
                                 145,
                                 146,
                                 147,
                                 148,
                                 149,
                                 150,
                                 151,
                                 152,
                                 153,
                                 154,
                                 155,
                                 156,
                                 157,
                                 158,
                                 159,
                                 160,
                                 161,
                                 162,
                                 163,
                                 164,
                                 165,
                                 166,
                                 167,
                                 168,
                                 169,
                                 170,
                                 171,
                                 172,
                                 173,
                                 174,
                                 175
            ));
        }};


        for (HotelWithOffers hotelWithOffers : hotelsWithOffers) {
            assertTrue(String.format(
                "returned wrong hotel, hotel %d is not in Munich",
                hotelWithOffers.getHotel().getId()
            ), hotelsInMunich.contains(hotelWithOffers.getHotel().getId()));
        }
    }
}

package com.mhv.batchprocessing.util;

import java.util.HashMap;
import java.util.Map;

public record LocationToCountryMap() {

    public static final Map<String, String> countryMap = new HashMap<>();

    static{
        countryMap.put(Location.AUS.name(), Country.AUSTRALIA.name());
        countryMap.put(Location.UK.name(), Country.UNITED_KINGDOM.name());
        countryMap.put(Location.IND.name(), Country.INDIA.name());
        countryMap.put(Location.USA.name(), Country.UNITED_STATES_OF_AMERICA.name());
        countryMap.put(Location.RUS.name(), Country.RUSSIA.name());
        countryMap.put(Location.CND.name(), Country.CANADA.name());
    }
}

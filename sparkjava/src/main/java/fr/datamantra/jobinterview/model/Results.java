package fr.datamantra.jobinterview.model;

import java.util.List;
import java.util.Map;

public class Results {

    private List<String> topTenEmittersLastTenYears;

    private Map<String, List<String>> smallestEmittersByYear;

    private List<String> topFiveIncreaseFrom1980to2000;

    public Results withSmallestEmittersByYear(Map<String, List<String>> value) {
        smallestEmittersByYear = value;
        return this;
    }

    public Results withTopTenEmittersLastTenYears(List<String> values) {
        topTenEmittersLastTenYears = values;
        return this;
    }

    public Results withTopFiveIncreaseFrom1980to2000(List<String> values) {
        topFiveIncreaseFrom1980to2000 = values;
        return this;
    }

    public List<String> getTopTenEmittersLastTenYears() {
        return topTenEmittersLastTenYears;
    }

    public Map<String, List<String>> getSmallestEmittersByYear() {
        return smallestEmittersByYear;
    }

    public List<String> getTopFiveIncreaseFrom1980to2000() {
        return topFiveIncreaseFrom1980to2000;
    }
}

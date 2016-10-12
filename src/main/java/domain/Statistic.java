package domain;

/**
 * Created by etheodor on 09/06/2016.
 */

public class Statistic {

    private String asset;
    private String statName;
    private Double statValue;

    public Statistic() {
    }


    public String getAsset() {
        return asset;
    }

    public void setAsset(String asset) {
        this.asset = asset;
    }

    public String getStatName() {
        return statName;
    }

    public void setStatName(String statName) {
        this.statName = statName;
    }

    public Double getStatValue() {
        return statValue;
    }

    public void setStatValue(Double statValue) {
        this.statValue = statValue;
    }

    @Override
    public String toString() {
        return "domain.Statistic{" +
                "asset='" + asset + '\'' +
                ", statName=" + statName +
                ", statValue=" + statValue +
                '}';
    }
}

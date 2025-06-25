package com.tabularasa.bi.q1_realtime_stream_processing.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Represents an advertisement event.
 * This class is a simple Plain Old Java Object (POJO) that is serializable.
 */
public class AdEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The unique identifier for the event.
     */
    private String eventId;

    /**
     * The identifier for the ad creative.
     */
    private String adCreativeId;

    /**
     * The identifier for the user.
     */
    private String userId;

    /**
     * The type of the event (e.g., "impression", "click").
     */
    private String eventType;

    /**
     * The timestamp of when the event occurred.
     */
    private LocalDateTime timestamp;

    /**
     * The type of device used for the event.
     */
    private String deviceType;

    /**
     * The country code of the user.
     */
    private String countryCode;

    /**
     * The spend in USD for the event.
     */
    private Double spendUsd;

    /**
     * The brand of the product.
     */
    private String productBrand;

    /**
     * The age group of the product.
     */
    private String productAgeGroup;

    /**
     * The first category of the product.
     */
    private Integer productCategory1;

    /**
     * The second category of the product.
     */
    private Integer productCategory2;

    /**
     * The third category of the product.
     */
    private Integer productCategory3;

    /**
     * The fourth category of the product.
     */
    private Integer productCategory4;

    /**
     * The fifth category of the product.
     */
    private Integer productCategory5;

    /**
     * The sixth category of the product.
     */
    private Integer productCategory6;

    /**
     * The seventh category of the product.
     */
    private Integer productCategory7;

    /**
     * The price of the product.
     */
    private Double productPrice;

    /**
     * The sales amount in Euro for the event.
     */
    private Double salesAmountEuro;

    /**
     * Indicates if the event is a sale.
     */
    private Boolean sale;

    /**
     * Gets the event ID.
     * @return The event ID.
     */
    public String getEventId() {
        return eventId;
    }

    /**
     * Sets the event ID.
     * @param eventId The event ID to set.
     */
    public void setEventId(final String eventId) {
        this.eventId = eventId;
    }

    /**
     * Gets the ad creative ID.
     * @return The ad creative ID.
     */
    public String getAdCreativeId() {
        return adCreativeId;
    }

    /**
     * Sets the ad creative ID.
     * @param adCreativeId The ad creative ID to set.
     */
    public void setAdCreativeId(final String adCreativeId) {
        this.adCreativeId = adCreativeId;
    }

    /**
     * Gets the user ID.
     * @return The user ID.
     */
    public String getUserId() {
        return userId;
    }

    /**
     * Sets the user ID.
     * @param userId The user ID to set.
     */
    public void setUserId(final String userId) {
        this.userId = userId;
    }

    /**
     * Gets the event type.
     * @return The event type.
     */
    public String getEventType() {
        return eventType;
    }

    /**
     * Sets the event type.
     * @param eventType The event type to set.
     */
    public void setEventType(final String eventType) {
        this.eventType = eventType;
    }

    /**
     * Gets the event timestamp.
     * @return The event timestamp.
     */
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the event timestamp.
     * @param timestamp The event timestamp to set.
     */
    public void setTimestamp(final LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Gets the device type.
     * @return The device type.
     */
    public String getDeviceType() {
        return deviceType;
    }

    /**
     * Sets the device type.
     * @param deviceType The device type to set.
     */
    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    /**
     * Gets the country code.
     * @return The country code.
     */
    public String getCountryCode() {
        return countryCode;
    }

    /**
     * Sets the country code.
     * @param countryCode The country code to set.
     */
    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    /**
     * Gets the spend in USD.
     * @return The spend in USD.
     */
    public Double getSpendUsd() {
        return spendUsd;
    }

    /**
     * Sets the spend in USD.
     * @param spendUsd The spend in USD to set.
     */
    public void setSpendUsd(Double spendUsd) {
        this.spendUsd = spendUsd;
    }

    /**
     * Gets the product brand.
     * @return The product brand.
     */
    public String getProductBrand() {
        return productBrand;
    }

    /**
     * Sets the product brand.
     * @param productBrand The product brand to set.
     */
    public void setProductBrand(String productBrand) {
        this.productBrand = productBrand;
    }

    /**
     * Gets the product age group.
     * @return The product age group.
     */
    public String getProductAgeGroup() {
        return productAgeGroup;
    }

    /**
     * Sets the product age group.
     * @param productAgeGroup The product age group to set.
     */
    public void setProductAgeGroup(String productAgeGroup) {
        this.productAgeGroup = productAgeGroup;
    }

    /**
     * Gets the first product category.
     * @return The first product category.
     */
    public Integer getProductCategory1() {
        return productCategory1;
    }

    /**
     * Sets the first product category.
     * @param productCategory1 The first product category to set.
     */
    public void setProductCategory1(Integer productCategory1) {
        this.productCategory1 = productCategory1;
    }

    /**
     * Gets the second product category.
     * @return The second product category.
     */
    public Integer getProductCategory2() {
        return productCategory2;
    }

    /**
     * Sets the second product category.
     * @param productCategory2 The second product category to set.
     */
    public void setProductCategory2(Integer productCategory2) {
        this.productCategory2 = productCategory2;
    }

    /**
     * Gets the third product category.
     * @return The third product category.
     */
    public Integer getProductCategory3() {
        return productCategory3;
    }

    /**
     * Sets the third product category.
     * @param productCategory3 The third product category to set.
     */
    public void setProductCategory3(Integer productCategory3) {
        this.productCategory3 = productCategory3;
    }

    /**
     * Gets the fourth product category.
     * @return The fourth product category.
     */
    public Integer getProductCategory4() {
        return productCategory4;
    }

    /**
     * Sets the fourth product category.
     * @param productCategory4 The fourth product category to set.
     */
    public void setProductCategory4(Integer productCategory4) {
        this.productCategory4 = productCategory4;
    }

    /**
     * Gets the fifth product category.
     * @return The fifth product category.
     */
    public Integer getProductCategory5() {
        return productCategory5;
    }

    /**
     * Sets the fifth product category.
     * @param productCategory5 The fifth product category to set.
     */
    public void setProductCategory5(Integer productCategory5) {
        this.productCategory5 = productCategory5;
    }

    /**
     * Gets the sixth product category.
     * @return The sixth product category.
     */
    public Integer getProductCategory6() {
        return productCategory6;
    }

    /**
     * Sets the sixth product category.
     * @param productCategory6 The sixth product category to set.
     */
    public void setProductCategory6(Integer productCategory6) {
        this.productCategory6 = productCategory6;
    }

    /**
     * Gets the seventh product category.
     * @return The seventh product category.
     */
    public Integer getProductCategory7() {
        return productCategory7;
    }

    /**
     * Sets the seventh product category.
     * @param productCategory7 The seventh product category to set.
     */
    public void setProductCategory7(Integer productCategory7) {
        this.productCategory7 = productCategory7;
    }

    /**
     * Gets the product price.
     * @return The product price.
     */
    public Double getProductPrice() {
        return productPrice;
    }

    /**
     * Sets the product price.
     * @param productPrice The product price to set.
     */
    public void setProductPrice(Double productPrice) {
        this.productPrice = productPrice;
    }

    /**
     * Gets the sales amount in Euro.
     * @return The sales amount in Euro.
     */
    public Double getSalesAmountEuro() {
        return salesAmountEuro;
    }

    /**
     * Sets the sales amount in Euro.
     * @param salesAmountEuro The sales amount in Euro to set.
     */
    public void setSalesAmountEuro(Double salesAmountEuro) {
        this.salesAmountEuro = salesAmountEuro;
    }

    /**
     * Gets the sale indicator.
     * @return The sale indicator.
     */
    public Boolean getSale() {
        return sale;
    }

    /**
     * Sets the sale indicator.
     * @param sale The sale indicator to set.
     */
    public void setSale(Boolean sale) {
        this.sale = sale;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AdEvent adEvent = (AdEvent) o;
        return Objects.equals(eventId, adEvent.eventId)
                && Objects.equals(adCreativeId, adEvent.adCreativeId)
                && Objects.equals(userId, adEvent.userId)
                && Objects.equals(eventType, adEvent.eventType)
                && Objects.equals(timestamp, adEvent.timestamp)
                && Objects.equals(deviceType, adEvent.deviceType)
                && Objects.equals(countryCode, adEvent.countryCode)
                && Objects.equals(spendUsd, adEvent.spendUsd)
                && Objects.equals(productBrand, adEvent.productBrand)
                && Objects.equals(productAgeGroup, adEvent.productAgeGroup)
                && Objects.equals(productCategory1, adEvent.productCategory1)
                && Objects.equals(productCategory2, adEvent.productCategory2)
                && Objects.equals(productCategory3, adEvent.productCategory3)
                && Objects.equals(productCategory4, adEvent.productCategory4)
                && Objects.equals(productCategory5, adEvent.productCategory5)
                && Objects.equals(productCategory6, adEvent.productCategory6)
                && Objects.equals(productCategory7, adEvent.productCategory7)
                && Objects.equals(productPrice, adEvent.productPrice)
                && Objects.equals(salesAmountEuro, adEvent.salesAmountEuro)
                && Objects.equals(sale, adEvent.sale);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, adCreativeId, userId, eventType, timestamp,
                deviceType, countryCode, spendUsd, productBrand, productAgeGroup,
                productCategory1, productCategory2, productCategory3, productCategory4,
                productCategory5, productCategory6, productCategory7,
                productPrice, salesAmountEuro, sale);
    }

    @Override
    public String toString() {
        return "AdEvent{"
                + "eventId='" + eventId + '\''
                + ", adCreativeId='" + adCreativeId + '\''
                + ", userId='" + userId + '\''
                + ", eventType='" + eventType + '\''
                + ", timestamp=" + timestamp
                + ", deviceType='" + deviceType + '\''
                + ", countryCode='" + countryCode + '\''
                + ", spendUsd=" + spendUsd
                + ", productBrand='" + productBrand + '\''
                + ", productAgeGroup='" + productAgeGroup + '\''
                + ", productCategory1=" + productCategory1
                + ", productCategory2=" + productCategory2
                + ", productCategory3=" + productCategory3
                + ", productCategory4=" + productCategory4
                + ", productCategory5=" + productCategory5
                + ", productCategory6=" + productCategory6
                + ", productCategory7=" + productCategory7
                + ", productPrice=" + productPrice
                + ", salesAmountEuro=" + salesAmountEuro
                + ", sale=" + sale
                + '}';
    }
}
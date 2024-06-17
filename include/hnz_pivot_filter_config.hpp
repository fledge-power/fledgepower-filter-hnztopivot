/*
 * FledgePower HNZ <-> pivot filter plugin.
 *
 * Copyright (c) 2022, RTE (https://www.rte-france.com)
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Michael Zillgith (michael.zillgith at mz-automation.de)
 * 
 */

#ifndef PIVOT_HNZ_CONFIG_H
#define PIVOT_HNZ_CONFIG_H

#include <string>
#include <map>
#include <memory>
#include <rapidjson/document.h>

#define FILTER_NAME "hnz_pivot_filter"

constexpr char JSON_NAME[] = "name";
constexpr char JSON_VERSION[] = "version";
constexpr char JSON_EXCHANGED_DATA_NAME[] = "exchanged_data";
constexpr char DATAPOINTS[] = "datapoints";
constexpr char LABEL[] = "label";
constexpr char PIVOT_ID[] = "pivot_id";
constexpr char PIVOT_TYPE[] = "pivot_type";
constexpr char PROTOCOLS[] = "protocols";
constexpr char HNZ_NAME[] = "hnzip";
constexpr char MESSAGE_CODE[] = "typeid";
constexpr char MESSAGE_ADDRESS[] = "address";
constexpr char MESSAGE_STATION[] = "station";

/**
 * @brief Checks if a string can be converted to an unsigned integer value.
 * 
 * This function attempts to convert the given string to an unsigned integer using std::stoul().
 * If the conversion succeeds and the resulting value is less than the maximum value representable
 * by an unsigned integer, it returns true; otherwise, it logs an error and returns false.
 * 
 * @param tmp The string to be converted.
 * @return True if the string can be converted to an unsigned integer, false otherwise.
 */
bool canConvertToUnsignedInt(const std::string& tmp);

/**
 * @brief Converts a string to an unsigned integer value.
 * 
 * This function first checks if the string can be converted to an unsigned integer using canConvertToUnsignedInt().
 * If the conversion is successful, it assigns the converted value to the 'value' parameter and returns true.
 * Otherwise, it logs an error and returns false.
 * 
 * @param tmp The string to be converted.
 * @param error_msg The error message to be logged in case of conversion failure.
 * @param msg Additional message to be logged in case of conversion failure.
 * @param value Reference to an unsigned integer where the converted value will be stored.
 * @return True if the string was successfully converted to an unsigned integer, false otherwise.
 */
bool convertToUnsignedInt(const std::string & tmp,const char *error_msg,const char *msg, unsigned int& value);

class HNZPivotDataPoint
{
public:
    /**
    * @brief Constructor of the HNZPivotDataPoint class.
    *
    * @param label The label of the data point.
    * @param pivotId The pivot identifier.
    * @param pivotType The type of the pivot.
    * @param typeIdStr The type identifier as string.
    * @param address The address of the data point.
    * @param station The station associated with the data point.
    */
    HNZPivotDataPoint(
    const std::string& label,
    const std::string& pivotId,
    const std::string& pivotType,
    const std::string& typeIdStr,
    unsigned int address,
    unsigned int station);

    const std::string& getLabel() const {return m_label;}
    const std::string& getPivotId() const {return m_pivotId;}
    const std::string& getPivotType() const {return m_pivotType;}
    const std::string& getTypeId() const {return m_typeIdStr;}
    unsigned int getAddress() const {return m_address;}

    /**
    * @brief Retrieves the station associated with the data point.
    *
    * @return The station associated with the data point.
    */
    unsigned int getStation() const {return m_station;}
private:
    std::string  m_label;
    std::string  m_pivotId;
    std::string  m_pivotType;

    std::string  m_typeIdStr;
    unsigned int m_address{0};
    unsigned int m_station{0};
};

class HNZPivotConfig
{
public:
    HNZPivotConfig() = default;

    void importExchangeConfig(const std::string& exchangeConfig);

    const std::map<std::string, std::shared_ptr<HNZPivotDataPoint>>& getExchangeDefinitions() const {return m_exchangeDefinitions;}
    std::string findPivotId(const std::string& typeIdStr, unsigned int address) const;
    static const std::string& getPluginName();
    bool isComplete() const {return m_exchange_data_is_complete;};

private:
    static bool m_check_string(const rapidjson::Value &json, const char *key);
    static bool m_check_array(const rapidjson::Value &json, const char *key);
    static bool m_check_object(const rapidjson::Value &json, const char *key);

    static bool m_retrieve(const rapidjson::Value &json, const char *key, unsigned int *target);
    static bool m_retrieve(const rapidjson::Value &json, const char *key, unsigned int *target, unsigned int def);
    static bool m_retrieve(const rapidjson::Value &json, const char *key, std::string *target);
    static bool m_retrieve(const rapidjson::Value &json, const char *key, std::string *target, const std::string& def);
    static bool m_retrieve(const rapidjson::Value &json, const char *key, long long int *target, long long int def);
    
    std::string m_getLookupHash(const std::string& typeIdStr, unsigned int address) const;
    
    bool m_exchange_data_is_complete = false;

    /* list of exchange data points -> the pivotId is the key */
    std::map<std::string, std::shared_ptr<HNZPivotDataPoint>> m_exchangeDefinitions;
    /* Map used to find the pivotId from the combination of typeid and address
       -> "typeid-address" is the key */
    std::map<std::string, std::string> m_pivotIdLookup;
};

#endif /* PIVOT_HNZ_CONFIG_H */
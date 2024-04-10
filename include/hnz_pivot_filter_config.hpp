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

class HNZPivotDataPoint
{
public:
    HNZPivotDataPoint(const std::string& label, const std::string& pivotId, const std::string& pivotType, const std::string& typeIdStr, unsigned int address, unsigned int station);

    const std::string& getLabel() const {return m_label;}
    const std::string& getPivotId() const {return m_pivotId;}
    const std::string& getPivotType() const {return m_pivotType;}
    const std::string& getTypeId() const {return m_typeIdStr;}
    unsigned int getAddress() const {return m_address;}
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
    HNZPivotDataPoint* getExchangeDefinitionsByLabel(const std::string& label);
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

    std::map<std::string, std::shared_ptr<HNZPivotDataPoint>> m_exchangeDefinitionsLabel;
};

#endif /* PIVOT_HNZ_CONFIG_H */
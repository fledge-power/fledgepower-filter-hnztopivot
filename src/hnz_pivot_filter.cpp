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

#include <plugin_api.h>

#include "hnz_pivot_filter.hpp"
#include "hnz_pivot_object.hpp"
#include "hnz_pivot_filter_config.hpp"
#include "hnz_pivot_utility.hpp"


HNZPivotFilter::HNZPivotFilter(const std::string& filterName, ConfigCategory& filterConfig,
                                OUTPUT_HANDLE *outHandle, OUTPUT_STREAM output):
        FledgeFilter(filterName, filterConfig, outHandle, output),
        m_config(std::make_shared<HNZPivotConfig>())
{
    (void)filterName; /* ignore parameter */
    readConfig(filterConfig);
}

HNZPivotFilter::~HNZPivotFilter()
{}

static bool checkLabelMatch(const std::string& incomingLabel, std::shared_ptr<HNZPivotDataPoint> exchangeConfig)
{
    return incomingLabel == exchangeConfig->getLabel();
}

static bool checkPivotTypeMatch(const std::string& incomingType, std::shared_ptr<HNZPivotDataPoint> exchangeConfig)
{
    const std::string& pivotType = exchangeConfig->getPivotType();
    if (incomingType == "TS") {
        return (pivotType == "SpsTyp") || (pivotType == "DpsTyp");
    }
    else if (incomingType == "TM") {
        return pivotType == "MvTyp";
    }
    else if ((incomingType == "TC") || (incomingType == "TVC")) {
        return (pivotType == "SpcTyp") || (pivotType == "DpcTyp") || (pivotType == "IncTyp");
    }
    return false;
}

static bool checkValueRange(const std::string& beforeLog, long value, long min, long max, const std::string& type)
{
    if (value < min || value > max) {
        PivotUtility::log_warn("%s do_value out of range [%ld..%ld] for %s: %ld", beforeLog.c_str(), min, max, type.c_str(), value);
        return false;
    }
    return true;
}

static void appendTimestamp(PivotObject& pivot, bool hasDoTs, unsigned long doTs, bool doTsIv, bool doTsS)
{
    if (hasDoTs) {
        pivot.addTimestamp(doTs, doTsS);
        pivot.addTmOrg(false);
        pivot.addTmValidity(doTsIv);
    }
    // For any message that does not have a timestamp in the protocol, add one artifically
    else {
        doTs = PivotTimestamp::getCurrentTimestampMs();
        pivot.addTimestamp(doTs, false);
        pivot.addTmOrg(true);
    }
}

Datapoint* HNZPivotFilter::convertDatapointToPivot(const std::string& assetName, Datapoint* sourceDp)
{
    Datapoint* convertedDatapoint = nullptr;
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - " + assetName + " - HNZPivotFilter::convertDatapointToPivot -";

    DatapointValue& dpv = sourceDp->getData();

    if (dpv.getType() != DatapointValue::T_DP_DICT)
        return nullptr;

    std::vector<Datapoint*>* datapoints = dpv.getDpVec();
    std::map<std::string, bool> attributeFound = {
        {"do_type", false},
        {"do_station", false},
        {"do_addr", false},
        {"do_value", false},
        {"do_valid", false},
        {"do_an", false},
        {"do_cg", false},
        {"do_outdated", false},
        {"do_ts", false},
        {"do_ts_iv", false},
        {"do_ts_c", false},
        {"do_ts_s", false},
    };

    std::string doType = "";
    unsigned int doStation = 0;
    unsigned int doAddress = 0;
    Datapoint* doValue = nullptr;
    unsigned int doValid = 0;
    std::string doAn = "";
    bool doCg = false;
    bool doOutdated = false;
    unsigned long doTs = 0;
    bool doTsIv = false;
    bool doTsC = false;
    bool doTsS = false;

    for (Datapoint* dp : *datapoints)
    {
        if ((!attributeFound["do_type"]) && (dp->getName() == "do_type")) {
            if (dp->getData().getType() == DatapointValue::T_STRING) {
                doType = dp->getData().toStringValue();
                attributeFound["do_type"] = true;
            }
        }
        else if ((!attributeFound["do_station"]) && (dp->getName() == "do_station")) {
            if (dp->getData().getType() == DatapointValue::T_INTEGER) {
                doStation = static_cast<unsigned int>(dp->getData().toInt());
                attributeFound["do_station"] = true;
            }
        }
        else if ((!attributeFound["do_addr"]) && (dp->getName() == "do_addr")) {
            if (dp->getData().getType() == DatapointValue::T_INTEGER) {
                doAddress = static_cast<unsigned int>(dp->getData().toInt());
                attributeFound["do_addr"] = true;
            }
        }
        else if ((!attributeFound["do_value"]) && (dp->getName() == "do_value"))
        {
            doValue = dp;
            attributeFound["do_value"] = true;
        }
        else if ((!attributeFound["do_valid"]) && (dp->getName() == "do_valid"))
        {
            if (dp->getData().getType() == DatapointValue::T_INTEGER) {
                doValid = static_cast<unsigned int>(dp->getData().toInt());
                attributeFound["do_valid"] = true;
            }
        }
        else if ((!attributeFound["do_an"]) && (dp->getName() == "do_an")) {
            if (dp->getData().getType() == DatapointValue::T_STRING) {
                doAn = dp->getData().toStringValue();
                attributeFound["do_an"] = true;
            }
        }
        else if ((!attributeFound["do_cg"]) && (dp->getName() == "do_cg"))
        {
            if (dp->getData().getType() == DatapointValue::T_INTEGER) {
                doCg = static_cast<bool>(dp->getData().toInt());
                attributeFound["do_cg"] = true;
            }
        }
        else if ((!attributeFound["do_outdated"]) && (dp->getName() == "do_outdated"))
        {
            if (dp->getData().getType() == DatapointValue::T_INTEGER) {
                doOutdated = static_cast<bool>(dp->getData().toInt());
                attributeFound["do_outdated"] = true;
            }
        }
        else if ((!attributeFound["do_ts"]) && (dp->getName() == "do_ts"))
        {
            if (dp->getData().getType() == DatapointValue::T_INTEGER) {
                doTs = dp->getData().toInt();
                attributeFound["do_ts"] = true;
            }
        }
        else if ((!attributeFound["do_ts_iv"]) && (dp->getName() == "do_ts_iv"))
        {
            if (dp->getData().getType() == DatapointValue::T_INTEGER) {
                doTsIv = static_cast<unsigned int>(dp->getData().toInt());
                attributeFound["do_ts_iv"] = true;
            }
        }
        else if ((!attributeFound["do_ts_c"]) && (dp->getName() == "do_ts_c"))
        {
            if (dp->getData().getType() == DatapointValue::T_INTEGER) {
                doTsC = static_cast<unsigned int>(dp->getData().toInt());
                attributeFound["do_ts_c"] = true;
            }
        }
        else if ((!attributeFound["do_ts_s"]) && (dp->getName() == "do_ts_s"))
        {
            if (dp->getData().getType() == DatapointValue::T_INTEGER) {
                doTsS = static_cast<unsigned int>(dp->getData().toInt());
                attributeFound["do_ts_s"] = true;
            }
        }
    }

    // Get exchangeConfig from message type and address
    if (!attributeFound["do_type"]) {
        PivotUtility::log_error("%s Missing do_type", beforeLog.c_str());
        return nullptr;
    }
    if (!attributeFound["do_addr"]) {
        PivotUtility::log_error("%s Missing do_addr", beforeLog.c_str());
        return nullptr;
    }
    const std::string& pivotId = m_config->findPivotId(doType, doAddress);
    if (pivotId.empty()) {
        PivotUtility::log_error("%s No pivot ID configured for typeid %s and address %u",
                                    beforeLog.c_str(), doType.c_str(), doAddress);
        return nullptr;
    }
    auto exchangeData = m_config->getExchangeDefinitions();
    if (exchangeData.count(pivotId) == 0) {
        PivotUtility::log_error("%s Unknown pivot ID: %s", beforeLog.c_str(), pivotId.c_str());
        return nullptr;
    }
    auto exchangeConfig = exchangeData[pivotId];
    if (!checkLabelMatch(assetName, exchangeConfig)) {
        PivotUtility::log_warn("%s Input label (%s) does not match configured label (%s) for pivot ID: %s",
                                    beforeLog.c_str(), assetName.c_str(), exchangeConfig->getLabel().c_str(), pivotId.c_str());   
    }

    //NOTE: when doValue is missing for a TS or TM, we are converting a quality reading
    
    if (doType == "TS") {
        // Message structure checks
        if (!checkPivotTypeMatch(doType, exchangeConfig)) {
            PivotUtility::log_warn("%s Invalid pivot type (%s) for data object type (%s)",
                                        beforeLog.c_str(), exchangeConfig->getPivotType().c_str(), doType.c_str());
        }
        if (!attributeFound["do_valid"]) {
            PivotUtility::log_warn("%s Missing attribute do_valid in TS", beforeLog.c_str());
        }
        if (!attributeFound["do_cg"]) {
            PivotUtility::log_warn("%s Missing attribute do_cg in TS", beforeLog.c_str());
        }
        else if (!doCg) {
            if (!attributeFound["do_ts"]) {
                PivotUtility::log_warn("%s Missing attribute do_ts in TS CE", beforeLog.c_str());
            }
            if (!attributeFound["do_ts_iv"]) {
                PivotUtility::log_warn("%s Missing attribute do_ts_iv in TS CE", beforeLog.c_str());
            }
            if (!attributeFound["do_ts_c"]) {
                PivotUtility::log_warn("%s Missing attribute do_ts_c in TS CE", beforeLog.c_str());
            }
            if (!attributeFound["do_ts_s"]) {
                PivotUtility::log_warn("%s Missing attribute do_ts_s in TS CE", beforeLog.c_str());
            }
        }
        if (!attributeFound["do_outdated"]) {
            PivotUtility::log_warn("%s Missing attribute do_outdated in TS", beforeLog.c_str());
        }
        else if (!doOutdated) {
            if (!attributeFound["do_value"]) {
                PivotUtility::log_warn("%s Missing attribute do_value in TS", beforeLog.c_str());
            }
        }
        // Pivot conversion
        const std::string& pivotType = exchangeConfig->getPivotType();
        PivotObject pivot("GTIS", pivotType);
        pivot.setIdentifier(exchangeConfig->getPivotId());
        pivot.setCause(doCg ? 20 : 3);
        
        if (attributeFound["do_value"]) {
            bool spsValue = false;
            if (doValue->getData().getType() == DatapointValue::T_INTEGER) {
                // Value range check
                long value = doValue->getData().toInt();
                checkValueRange(beforeLog, value, 0, 1, "TS");
                spsValue = static_cast<bool>(value);
            }
            // Fill TS Double field from TS Simple infos
            if (pivotType == "DpsTyp") {
                 pivot.setStValStr(spsValue?"on":"off");
            }
            else {
                pivot.setStVal(spsValue);
            }
        }

        pivot.addQuality(doValid, doOutdated, doTsC, doTsS);

        appendTimestamp(pivot, attributeFound["do_ts"], doTs, doTsIv, doTsS);
        
        convertedDatapoint = pivot.toDatapoint();
    }
    else if (doType  == "TM") {
        // Message structure checks
        if (!checkPivotTypeMatch(doType, exchangeConfig)) {
            PivotUtility::log_warn("%s Invalid pivot type (%s) for data object type (%s)",
                                        beforeLog.c_str(), exchangeConfig->getPivotType().c_str(), doType.c_str());
        }
        if (!attributeFound["do_valid"]) {
            PivotUtility::log_warn("%s Missing attribute do_valid in TM", beforeLog.c_str());
        }
        if (!attributeFound["do_an"]) {
            PivotUtility::log_warn("%s Missing attribute do_an in TM", beforeLog.c_str());
        }
        if (!attributeFound["do_outdated"]) {
            PivotUtility::log_warn("%s Missing attribute do_outdated in TM", beforeLog.c_str());
        }
        else if (!doOutdated) {
            if (!attributeFound["do_value"]) {
                PivotUtility::log_warn("%s Missing attribute do_value in TM", beforeLog.c_str());
            }
        }
        // Pivot conversion
        PivotObject pivot("GTIM", exchangeConfig->getPivotType());
        pivot.setIdentifier(exchangeConfig->getPivotId());
        pivot.setCause(1);
        
        if (attributeFound["do_value"]) {
            if (doValue->getData().getType() == DatapointValue::T_INTEGER) {
                // Value range check
                long value = doValue->getData().toInt();
                if (attributeFound["do_an"]) {
                    if (doAn == "TMA") {
                        checkValueRange(beforeLog, value, -127, 127, doAn);
                    }
                    else if (doAn == "TM8") {
                        checkValueRange(beforeLog, value, 0, 255, doAn);
                    }
                    else if (doAn == "TM16") {
                        checkValueRange(beforeLog, value, -32768, 32767, doAn);
                    }
                    else {
                        PivotUtility::log_warn("%s Unknown do_an: %s", beforeLog.c_str(), doAn.c_str());
                    }
                }
                pivot.setMagI(static_cast<int>(value));
            }
        }

        pivot.addQuality(doValid, doOutdated, false, false);

        appendTimestamp(pivot, false, 0, false, false);
        
        convertedDatapoint = pivot.toDatapoint();
    }
    else if (doType == "TC") // Acknowledgment of a TC
    {
        // Message structure checks
        const std::string& pivotType = exchangeConfig->getPivotType();
        if (!checkPivotTypeMatch(doType, exchangeConfig)) {
            PivotUtility::log_warn("%s Invalid pivot type (%s) for data object type (%s)",
                                        beforeLog.c_str(), pivotType.c_str(), doType.c_str());
        }
        if (!attributeFound["do_valid"]) {
            PivotUtility::log_warn("%s Missing attribute do_valid in TC ACK", beforeLog.c_str());
        }
        // Pivot conversion
        
        PivotObject pivot("GTIC", pivotType);
        pivot.setIdentifier(exchangeConfig->getPivotId());
        pivot.setCause(7);
        
        pivot.addQuality(doValid, false, false, false);

        appendTimestamp(pivot, false, 0, false, false);
        
        convertedDatapoint = pivot.toDatapoint();
    }
    else if (doType == "TVC") // Acknowledgment of a TVC
    {
        // Message structure checks
        if (!checkPivotTypeMatch(doType, exchangeConfig)) {
            PivotUtility::log_warn("%s Invalid pivot type (%s) for data object type (%s)",
                                        beforeLog.c_str(), exchangeConfig->getPivotType().c_str(), doType.c_str());
        }
        if (!attributeFound["do_valid"]) {
            PivotUtility::log_warn("%s Missing attribute do_valid in TVC ACK", beforeLog.c_str());
        }
        // Pivot conversion
        PivotObject pivot("GTIC", exchangeConfig->getPivotType());
        pivot.setIdentifier(exchangeConfig->getPivotId());
        pivot.setCause(7);
        
        pivot.addQuality(doValid, false, false, false);

        appendTimestamp(pivot, false, 0, false, false);
        
        convertedDatapoint = pivot.toDatapoint();
    }
    else {
        PivotUtility::log_error("%s Unknown do_type: %s", beforeLog.c_str(), doType.c_str());
        return nullptr;
    }

    return convertedDatapoint;
}

Datapoint* HNZPivotFilter::convertDatapointToHNZ(const std::string& assetName, Datapoint* sourceDp)
{
    Datapoint* convertedDatapoint = nullptr;
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - " + assetName + " - HNZPivotFilter::convertDatapointToHNZ -";

    try {
        PivotObject pivotObject(sourceDp);
        auto exchangeData = m_config->getExchangeDefinitions();
        const std::string& pivotId = pivotObject.getIdentifier();
        if (exchangeData.count(pivotId) == 0) {
            std::vector<std::string> pivotIds;
            for(auto const& kvp: exchangeData) {
                pivotIds.push_back(kvp.first);
            }
            PivotUtility::log_error("%s Unknown pivot ID: %s (available: %s)",
                                        beforeLog.c_str(), pivotId.c_str(), PivotUtility::join(pivotIds).c_str());
            return nullptr;
        }
        auto exchangeConfig = exchangeData[pivotId];
        if (!checkLabelMatch(assetName, exchangeConfig)) {
            PivotUtility::log_warn("%s Input label (%s) does not match configured label (%s) for pivot ID: %s",
                                        beforeLog.c_str(), assetName.c_str(), exchangeConfig->getLabel().c_str(), pivotId.c_str());
        }
        convertedDatapoint = pivotObject.toHnzCommandObject(exchangeConfig);
    }
    catch (PivotObjectException& e)
    {
        PivotUtility::log_error("%s Failed to convert pivot object: %s", beforeLog.c_str(), e.getContext().c_str());
    }

    return convertedDatapoint;
}

void HNZPivotFilter::ingest(READINGSET* readingSet)
{
    std::lock_guard<std::recursive_mutex> guard(m_configMutex);
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - HNZPivotFilter::ingest -";
    if (!isEnabled()) {
        return;
    }
    if (!readingSet) {
        PivotUtility::log_error("%s No reading set provided", beforeLog.c_str());
        return;
    }
    /* apply transformation */
    std::vector<Reading*>* readings = readingSet->getAllReadingsPtr();

    PivotUtility::log_info("%s %d readings", beforeLog.c_str(), readings->size());

    std::vector<Reading*>::iterator readIt = readings->begin();

    while(readIt != readings->end())
    {
        Reading* reading = *readIt;

        std::string assetName = reading->getAssetName();
        beforeLog = HNZPivotConfig::getPluginName() + " - " + assetName + " - HNZPivotFilter::ingest -";

        std::vector<Datapoint*>& datapoints = reading->getReadingData();

        std::vector<Datapoint*> convertedDatapoints;

        PivotUtility::log_debug("%s original Reading: %s", beforeLog.c_str(), reading->toJSON().c_str());

        for (Datapoint* dp : datapoints) {
            if (dp->getName() == "data_object") {
                Datapoint* convertedDp = convertDatapointToPivot(assetName, dp);

                if (convertedDp) {
                    convertedDatapoints.push_back(convertedDp);
                }
                else {
                    PivotUtility::log_error("%s Failed to convert data_object", beforeLog.c_str());
                }
            }
            else if (dp->getName() == "PIVOT") {
                Datapoint* convertedDp = convertDatapointToHNZ(assetName, dp);

                if (convertedDp) {
                    convertedDatapoints.push_back(convertedDp);
                }
                else {
                    PivotUtility::log_error("%s Failed to convert PIVOT object", beforeLog.c_str());
                }
            }
            else if (dp->getName() == "south_event") {
                PivotUtility::log_debug("%s Forwarding south_event unchanged", beforeLog.c_str());
                convertedDatapoints.push_back(new Datapoint(dp->getName(), dp->getData()));
            }
            else {
                PivotUtility::log_debug("%s Unknown reading type: %s, message removed", beforeLog.c_str(), dp->getName().c_str());
            }
        }

        reading->removeAllDatapoints();

        for (Datapoint* convertedDatapoint : convertedDatapoints) {
            reading->addDatapoint(convertedDatapoint);
        }

        PivotUtility::log_debug("%s converted Reading: %s", beforeLog.c_str(), reading->toJSON().c_str());

        if (reading->getReadingData().size() == 0) {
            readIt = readings->erase(readIt);
        }
        else {
            readIt++;
        }
    }

    if (!readings->empty())
    {
        if (m_func) {
            PivotUtility::log_debug("%s Send %lu converted readings", beforeLog.c_str(), readings->size());

            m_func(m_data, readingSet);
        }
        else {
            PivotUtility::log_error("%s No function to call, discard %lu converted readings", beforeLog.c_str(), readings->size());
        }
    }
}

void HNZPivotFilter::reconfigure(const std::string& newConfig) {
    std::lock_guard<std::recursive_mutex> guard(m_configMutex);
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - HNZPivotFilter::reconfigure -";
    PivotUtility::log_debug("%s reconfigure called", beforeLog.c_str());
    setConfig(newConfig);

    ConfigCategory config("hnzpivot", newConfig);
    readConfig(config);
}

void HNZPivotFilter::readConfig(ConfigCategory& config) {
    std::lock_guard<std::recursive_mutex> guard(m_configMutex);
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - HNZPivotFilter::readConfig -";
    if (config.itemExists("exchanged_data")) {
        const std::string exchangedData = config.getValue("exchanged_data");
        m_config->importExchangeConfig(exchangedData);
    }
    else {
        PivotUtility::log_error("%s Missing exchanged_data configuation", beforeLog.c_str());
    }
}

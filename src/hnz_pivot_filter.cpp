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
        m_filterConfig(std::make_shared<HNZPivotConfig>())
{
    (void)filterName; /* ignore parameter */
    readConfig(filterConfig);
}

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

template <typename T>
void HNZPivotFilter::readAttribute(std::map<std::string, bool>& attributeFound, Datapoint* dp,
                                          const std::string& targetName, T& out) {
    const auto& name = dp->getName();
    if (name != targetName) {
        return;
    }
    if (attributeFound[name]) {
        return;
    }

    if (dp->getData().getType() == DatapointValue::T_INTEGER) {
        out = static_cast<T>(dp->getData().toInt());
        attributeFound[name] = true;
    }
}

void HNZPivotFilter::readAttribute(std::map<std::string, bool>& attributeFound, Datapoint* dp,
                                               const std::string& targetName, Datapoint*& out) {
    const auto& name = dp->getName();
    if (name != targetName) {
        return;
    }
    if (attributeFound[name]) {
        return;
    }

    out = dp;
    attributeFound[name] = true;
}

void HNZPivotFilter::readAttribute(std::map<std::string, bool>& attributeFound, Datapoint* dp,
                                                const std::string& targetName, std::string& out) {
    const auto& name = dp->getName();
    if (name != targetName) {
        return;
    }
    if (attributeFound[name]) {
        return;
    }

    if (dp->getData().getType() == DatapointValue::T_STRING) {
        out = dp->getData().toStringValue();
        attributeFound[name] = true;
    }
}

Datapoint* HNZPivotFilter::convertDatapointToPivot(const std::string& assetName, Datapoint* sourceDp)
{
    Datapoint* convertedDatapoint = nullptr;
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - " + assetName + " - HNZPivotFilter::convertDatapointToPivot -";

    DatapointValue& dpv = sourceDp->getData();

    if (dpv.getType() != DatapointValue::T_DP_DICT)
        return nullptr;

    const std::vector<Datapoint*>* datapoints = dpv.getDpVec();
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

    GenericDataObject dataObject;

    for (Datapoint* dp : *datapoints)
    {
        readAttribute(attributeFound, dp, "do_type", dataObject.doType);
        readAttribute(attributeFound, dp, "do_station", dataObject.doStation);
        readAttribute(attributeFound, dp, "do_addr", dataObject.doAddress);
        readAttribute(attributeFound, dp, "do_value", dataObject.doValue);
        readAttribute(attributeFound, dp, "do_valid", dataObject.doValid);
        readAttribute(attributeFound, dp, "do_an", dataObject.doAn);
        readAttribute(attributeFound, dp, "do_cg", dataObject.doCg);
        readAttribute(attributeFound, dp, "do_outdated", dataObject.doOutdated);
        readAttribute(attributeFound, dp, "do_ts", dataObject.doTs);
        readAttribute(attributeFound, dp, "do_ts_iv", dataObject.doTsIv);
        readAttribute(attributeFound, dp, "do_ts_c", dataObject.doTsC);
        readAttribute(attributeFound, dp, "do_ts_s", dataObject.doTsS);
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
    const std::string& pivotId = m_filterConfig->findPivotId(dataObject.doType, dataObject.doAddress);
    if (pivotId.empty()) {
        PivotUtility::log_error("%s No pivot ID configured for typeid %s and address %u",
                                    beforeLog.c_str(), dataObject.doType.c_str(), dataObject.doAddress);
        return nullptr;
    }
    auto exchangeData = m_filterConfig->getExchangeDefinitions();
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
    
    if (dataObject.doType == "TS") {
        convertedDatapoint = convertTSToPivot(assetName, attributeFound, dataObject, exchangeConfig);
    }
    else if (dataObject.doType  == "TM") {
        convertedDatapoint = convertTMToPivot(assetName, attributeFound, dataObject, exchangeConfig); 
    }
    else if (dataObject.doType == "TC") // Acknowledgment of a TC
    {
        convertedDatapoint = convertTCACKToPivot(assetName, attributeFound, dataObject, exchangeConfig); 
    }
    else if (dataObject.doType == "TVC") // Acknowledgment of a TVC
    {
        convertedDatapoint = convertTVCACKToPivot(assetName, attributeFound, dataObject, exchangeConfig);
    }
    else {
        PivotUtility::log_error("%s Unknown do_type: %s", beforeLog.c_str(), dataObject.doType.c_str());
        return nullptr;
    }

    return convertedDatapoint;
}

Datapoint* HNZPivotFilter::convertTSToPivot(const std::string& assetName, std::map<std::string, bool>& attributeFound,
                                            const GenericDataObject& dataObject, std::shared_ptr<HNZPivotDataPoint> exchangeConfig)
{
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - " + assetName + " - HNZPivotFilter::convertTSToPivot -";

    // Message structure checks
    if (!checkPivotTypeMatch(dataObject.doType, exchangeConfig)) {
        PivotUtility::log_warn("%s Invalid pivot type (%s) for data object type (%s)",
                                    beforeLog.c_str(), exchangeConfig->getPivotType().c_str(), dataObject.doType.c_str());
    }
    if (!attributeFound["do_valid"]) {
        PivotUtility::log_warn("%s Missing attribute do_valid in TS", beforeLog.c_str());
    }
    if (!attributeFound["do_cg"]) {
        PivotUtility::log_warn("%s Missing attribute do_cg in TS", beforeLog.c_str());
    }
    else if (!dataObject.doCg) {
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
    else if (!dataObject.doOutdated && !attributeFound["do_value"]) {
        PivotUtility::log_warn("%s Missing attribute do_value in TS", beforeLog.c_str());
    }
    // Pivot conversion
    const std::string& pivotType = exchangeConfig->getPivotType();
    PivotObject pivot("GTIS", pivotType);
    pivot.setIdentifier(exchangeConfig->getPivotId());
    pivot.setCause(dataObject.doCg ? 20 : 3);
    
    if (attributeFound["do_value"]) {
        bool spsValue = false;
        if (dataObject.doValue->getData().getType() == DatapointValue::T_INTEGER) {
            // Value range check
            long value = dataObject.doValue->getData().toInt();
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

    pivot.addQuality(dataObject.doValid, dataObject.doOutdated, dataObject.doTsC, dataObject.doTsS);

    appendTimestamp(pivot, attributeFound["do_ts"], dataObject.doTs, dataObject.doTsIv, dataObject.doTsS);
    
    return pivot.toDatapoint();
}


Datapoint* HNZPivotFilter::convertTMToPivot(const std::string& assetName, std::map<std::string, bool>& attributeFound,
                                            const GenericDataObject& dataObject, std::shared_ptr<HNZPivotDataPoint> exchangeConfig)
{
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - " + assetName + " - HNZPivotFilter::convertTMToPivot -";

    // Message structure checks
    if (!checkPivotTypeMatch(dataObject.doType, exchangeConfig)) {
        PivotUtility::log_warn("%s Invalid pivot type (%s) for data object type (%s)",
                                    beforeLog.c_str(), exchangeConfig->getPivotType().c_str(), dataObject.doType.c_str());
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
    else if (!dataObject.doOutdated && !attributeFound["do_value"]) {
        PivotUtility::log_warn("%s Missing attribute do_value in TM", beforeLog.c_str());
    }
    // Pivot conversion
    PivotObject pivot("GTIM", exchangeConfig->getPivotType());
    pivot.setIdentifier(exchangeConfig->getPivotId());
    pivot.setCause(1);
    
    if (attributeFound["do_value"] && (dataObject.doValue->getData().getType() == DatapointValue::T_INTEGER)) {
        // Value range check
        long value = dataObject.doValue->getData().toInt();
        if (attributeFound["do_an"]) {
            if (dataObject.doAn == "TMA") {
                checkValueRange(beforeLog, value, -127, 127, dataObject.doAn);
            }
            else if (dataObject.doAn == "TM8") {
                checkValueRange(beforeLog, value, 0, 255, dataObject.doAn);
            }
            else if (dataObject.doAn == "TM16") {
                checkValueRange(beforeLog, value, -32768, 32767, dataObject.doAn);
            }
            else {
                PivotUtility::log_warn("%s Unknown do_an: %s", beforeLog.c_str(), dataObject.doAn.c_str());
            }
        }
        pivot.setMagI(static_cast<int>(value));
    }

    pivot.addQuality(dataObject.doValid, dataObject.doOutdated, false, false);

    appendTimestamp(pivot, false, 0, false, false);
        
    return pivot.toDatapoint();
}

Datapoint* HNZPivotFilter::convertTCACKToPivot(const std::string& assetName, std::map<std::string, bool>& attributeFound,
                                               const GenericDataObject& dataObject, std::shared_ptr<HNZPivotDataPoint> exchangeConfig)
{
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - " + assetName + " - HNZPivotFilter::convertTCACKToPivot -";

    // Message structure checks
    const std::string& pivotType = exchangeConfig->getPivotType();
    if (!checkPivotTypeMatch(dataObject.doType, exchangeConfig)) {
        PivotUtility::log_warn("%s Invalid pivot type (%s) for data object type (%s)",
                                    beforeLog.c_str(), pivotType.c_str(), dataObject.doType.c_str());
    }
    if (!attributeFound["do_valid"]) {
        PivotUtility::log_warn("%s Missing attribute do_valid in TC ACK", beforeLog.c_str());
    }
    // Pivot conversion
    
    PivotObject pivot("GTIC", pivotType);
    pivot.setIdentifier(exchangeConfig->getPivotId());
    pivot.setCause(7);
    
    pivot.addQuality(dataObject.doValid, false, false, false);

    appendTimestamp(pivot, false, 0, false, false);
        
    return pivot.toDatapoint();
}

Datapoint* HNZPivotFilter::convertTVCACKToPivot(const std::string& assetName, std::map<std::string, bool>& attributeFound,
                                                const GenericDataObject& dataObject, std::shared_ptr<HNZPivotDataPoint> exchangeConfig)
{
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - " + assetName + " - HNZPivotFilter::convertTVCACKToPivot -";

    // Message structure checks
    if (!checkPivotTypeMatch(dataObject.doType, exchangeConfig)) {
        PivotUtility::log_warn("%s Invalid pivot type (%s) for data object type (%s)",
                                    beforeLog.c_str(), exchangeConfig->getPivotType().c_str(), dataObject.doType.c_str());
    }
    if (!attributeFound["do_valid"]) {
        PivotUtility::log_warn("%s Missing attribute do_valid in TVC ACK", beforeLog.c_str());
    }
    // Pivot conversion
    PivotObject pivot("GTIC", exchangeConfig->getPivotType());
    pivot.setIdentifier(exchangeConfig->getPivotId());
    pivot.setCause(7);
    
    pivot.addQuality(dataObject.doValid, false, false, false);

    appendTimestamp(pivot, false, 0, false, false);
        
    return pivot.toDatapoint();
}

Datapoint* HNZPivotFilter::convertDatapointToHNZ(const std::string& assetName, Datapoint* sourceDp)
{
    Datapoint* convertedDatapoint = nullptr;
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - " + assetName + " - HNZPivotFilter::convertDatapointToHNZ -";

    try {
        PivotObject pivotObject(sourceDp);
        auto exchangeData = m_filterConfig->getExchangeDefinitions();
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

void HNZPivotFilter::convertDatapoint(const std::string& assetName, Datapoint* dp, std::vector<Datapoint*>& convertedDatapoints) {
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - HNZPivotFilter::processDatapoint -";
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

    auto readIt = readings->begin();

    while(readIt != readings->end())
    {
        Reading* reading = *readIt;

        std::string assetName = reading->getAssetName();
        beforeLog = HNZPivotConfig::getPluginName() + " - " + assetName + " - HNZPivotFilter::ingest -";

        const std::vector<Datapoint*>& datapoints = reading->getReadingData();

        std::vector<Datapoint*> convertedDatapoints;

        PivotUtility::log_debug("%s original Reading: %s", beforeLog.c_str(), reading->toJSON().c_str());

        for (Datapoint* dp : datapoints) {
            convertDatapoint(assetName, dp, convertedDatapoints);
        }

        reading->removeAllDatapoints();

        for (Datapoint* convertedDatapoint : convertedDatapoints) {
            reading->addDatapoint(convertedDatapoint);
        }

        PivotUtility::log_debug("%s converted Reading: %s", beforeLog.c_str(), reading->toJSON().c_str());

        if (reading->getReadingData().empty()) {
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

void HNZPivotFilter::readConfig(const ConfigCategory& config) {
    std::lock_guard<std::recursive_mutex> guard(m_configMutex);
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - HNZPivotFilter::readConfig -";
    if (config.itemExists("exchanged_data")) {
        const std::string exchangedData = config.getValue("exchanged_data");
        m_filterConfig->importExchangeConfig(exchangedData);
    }
    else {
        PivotUtility::log_error("%s Missing exchanged_data configuation", beforeLog.c_str());
    }
}

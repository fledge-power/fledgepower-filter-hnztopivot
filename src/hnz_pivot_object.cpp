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

#include <chrono>
#include <cmath>
#include <datapoint.h>

#include "hnz_pivot_utility.hpp"
#include "hnz_pivot_object.hpp"
#include "hnz_pivot_filter_config.hpp"


static Datapoint* createDp(const std::string& name)
{
    auto datapoints = new std::vector<Datapoint*>;

    DatapointValue dpv(datapoints, true);

    auto dp = new Datapoint(name, dpv);

    return dp;
}

template <class T>
static Datapoint* createDpWithValue(const std::string& name, const T value)
{
    DatapointValue dpv(value);

    auto dp = new Datapoint(name, dpv);

    return dp;
}

static Datapoint* addElement(Datapoint* dp, const std::string& name)
{
    DatapointValue& dpv = dp->getData();

    std::vector<Datapoint*>* subDatapoints = dpv.getDpVec();

    Datapoint* element = createDp(name);

    if (element) {
       subDatapoints->push_back(element);
    }

    return element;
}

template <class T>
static Datapoint* addElementWithValue(Datapoint* dp, const std::string& name, const T value)
{
    DatapointValue& dpv = dp->getData();

    std::vector<Datapoint*>* subDatapoints = dpv.getDpVec();

    Datapoint* element = createDpWithValue(name, value);

    if (element) {
       subDatapoints->push_back(element);
    }

    return element;
}

static Datapoint* getChild(Datapoint* dp, const std::string& name)
{
    Datapoint* childDp = nullptr;

    if (dp != nullptr && dp->getData().getType() == DatapointValue::T_DP_DICT) {

        DatapointValue& dpv = dp->getData();
        const std::vector<Datapoint*>* datapoints = dpv.getDpVec();
    
        for (Datapoint* child : *datapoints) {
            if (child->getName() == name) {
                childDp = child;
                break;
            }
        }
    }

    return childDp;
}

static std::string getValueStr(Datapoint* dp)
{
    const DatapointValue& dpv = dp->getData();

    if (dpv.getType() == DatapointValue::T_STRING) {
        return dpv.toStringValue();
    }
    else {
        throw HnzPivotObjectException("datapoint " + dp->getName() + " has not a string value");
    }
}

static std::string getChildValueStr(Datapoint* dp, const std::string& name)
{
    Datapoint* childDp = getChild(dp, name);

    if (childDp) {
        return getValueStr(childDp);
    }
    else {
        throw HnzPivotObjectException("No such child: " + name);
    }
}

static long getValueLong(Datapoint* dp)
{
    const DatapointValue& dpv = dp->getData();

    if (dpv.getType() == DatapointValue::T_INTEGER) {
        return dpv.toInt();
    }
    else {
        throw HnzPivotObjectException("datapoint " + dp->getName() + " has not an int value");
    }
}

static long getChildValueLong(Datapoint* dp, const std::string& name)
{
    Datapoint* childDp = getChild(dp, name);

    if (childDp) {
        return getValueLong(childDp);
    }
    else {
        throw HnzPivotObjectException("No such child: " + name);
    }
}

static int getValueInt(Datapoint* dp)
{
    return static_cast<int>(getValueLong(dp));
}

static double getValueFloat(Datapoint* dp)
{
    const DatapointValue& dpv = dp->getData();

    if (dpv.getType() == DatapointValue::T_FLOAT) {
        return dpv.toDouble();
    }
    else {
        throw HnzPivotObjectException("datapoint " + dp->getName() + " has not an float value");
    }
}

static int getChildValueInt(Datapoint* dp, const std::string& name)
{
    return static_cast<int>(getChildValueLong(dp, name));
}

void HnzPivotTimestamp::handleTimeQuality(Datapoint* timeQuality)
{
    DatapointValue& dpv = timeQuality->getData();
    if (dpv.getType() != DatapointValue::T_DP_DICT) {
        return;
    }

    const std::vector<Datapoint*>* datapoints = dpv.getDpVec();
    for (Datapoint* child : *datapoints) {
        if (child->getName() == "clockFailure") {
            if (getValueInt(child) > 0)
                m_clockFailure = true;
            else
                m_clockFailure = false;
        }
        else if (child->getName() == "clockNotSynchronized") {
            if (getValueInt(child) > 0)
                m_clockNotSynchronized = true;
            else
                m_clockNotSynchronized = false;
        }
        else if (child->getName() == "leapSecondKnown") {
            if (getValueInt(child) > 0)
                m_leapSecondKnown = true;
            else
                m_leapSecondKnown = false;
        }
        else if (child->getName() == "timeAccuracy") {
            m_timeAccuracy = getValueInt(child);
        }
    }
}

HnzPivotTimestamp::HnzPivotTimestamp(Datapoint* timestampData)
{
    DatapointValue& dpv = timestampData->getData();
    if (dpv.getType() != DatapointValue::T_DP_DICT) {
        return;

    }

    const std::vector<Datapoint*>* datapoints = dpv.getDpVec();
    for (Datapoint* child : *datapoints)
    {
        if (child->getName() == "SecondSinceEpoch") {
            m_secondSinceEpoch = getValueInt(child);
        }
        else if (child->getName() == "FractionOfSecond") {
            m_fractionOfSecond = getValueInt(child);
        }
        else if (child->getName() == "TimeQuality") {
            handleTimeQuality(child);
        }
    }
}

long HnzPivotTimestamp::toTimestamp(long secondSinceEpoch, long fractionOfSecond) {
    long timestamp = 0;
    auto msPart = static_cast<long>(round(static_cast<double>(fractionOfSecond * 1000) / 16777216.0));
    timestamp = (secondSinceEpoch * 1000L) + msPart;
    return timestamp;
}

std::pair<long, long> HnzPivotTimestamp::fromTimestamp(long timestamp) {
    long remainder = (timestamp % 1000L);
    long fractionOfSecond = remainder * 16777 + ((remainder * 216) / 1000);
    return std::make_pair(timestamp / 1000L, fractionOfSecond);
}

uint64_t HnzPivotTimestamp::getCurrentTimestampMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

Datapoint* HnzPivotObject::getCdc(Datapoint* dp)
{
    Datapoint* cdcDp = nullptr;
    std::vector<std::string> unknownChildrenNames;

    DatapointValue& dpv = dp->getData();
    if (dpv.getType() != DatapointValue::T_DP_DICT) {
        throw HnzPivotObjectException("CDC type missing");
    }

    const std::vector<Datapoint*>* datapoints = dpv.getDpVec();
    for (Datapoint* child : *datapoints) {
        if (child->getName() == "SpsTyp") {
            cdcDp = child;
            m_pivotCdc = HnzPivotCdc::SPS;
        }
        else if (child->getName() == "MvTyp") {
            cdcDp = child;
            m_pivotCdc = HnzPivotCdc::MV;
        }
        else if (child->getName() == "DpsTyp") {
            cdcDp = child;
            m_pivotCdc = HnzPivotCdc::DPS;
        }
        else if (child->getName() == "SpcTyp") {
            cdcDp = child;
            m_pivotCdc = HnzPivotCdc::SPC;
        }
        else if (child->getName() == "DpcTyp") {
            cdcDp = child;
            m_pivotCdc = HnzPivotCdc::DPC;
        }
        else if (child->getName() == "IncTyp") {
            cdcDp = child;
            m_pivotCdc = HnzPivotCdc::INC;
        }
        else {
            unknownChildrenNames.push_back(child->getName());
        }
        if (cdcDp != nullptr) {
            break;
        }
    }

    if(cdcDp == nullptr) {
        throw HnzPivotObjectException("CDC type unknown: " + HnzPivotUtility::join(unknownChildrenNames));
    }
    if(!checkCdcTypeMatch(m_pivotCdc, m_pivotClass)) {
        throw HnzPivotObjectException("CDC type (" + HnzPivotCdcStr(m_pivotCdc) + ") does not match pivot class ("
                                    + HnzPivotClassStr(m_pivotClass) + ")");
    }
    
    return cdcDp;
}

bool HnzPivotObject::readBool(Datapoint* dp, const std::string& name, bool& out) const
{
    if (dp->getName() == name) {
        if (getValueInt(dp) > 0)
            out = true;
        else
            out = false;
        return true;
    }
    return false;
}

void HnzPivotObject::handleDetailQuality(Datapoint* detailQuality)
{
    DatapointValue& dpv = detailQuality->getData();
    if (dpv.getType() != DatapointValue::T_DP_DICT) {
        return;
    }

    const std::vector<Datapoint*>* datapoints = dpv.getDpVec();
    for (Datapoint* child : *datapoints) {
        if(readBool(child, "badReference", m_badReference)) continue;
        if(readBool(child, "failure", m_failure)) continue;
        if(readBool(child, "inconsistent", m_inconsistent)) continue;
        if(readBool(child, "inacurate", m_inacurate)) continue;
        if(readBool(child, "oldData", m_oldData)) continue;
        if(readBool(child, "oscillatory", m_oscillatory)) continue;
        if(readBool(child, "outOfRange", m_outOfRange)) continue;
        if(readBool(child, "overflow", m_overflow)) continue;
    }
}

void HnzPivotObject::handleQuality(Datapoint* q)
{
    DatapointValue& dpv = q->getData();
    if (dpv.getType() != DatapointValue::T_DP_DICT) {
        return;
    }

    const std::vector<Datapoint*>* datapoints = dpv.getDpVec();
    for (Datapoint* child : *datapoints) {
        if (child->getName() == "Validity") {
            std::string validityStr = getValueStr(child);
            if (validityStr == "good") {
                continue;
            }

            if (validityStr == "invalid") {
                m_validity = HnzValidity::INVALID;
            }
            else if (validityStr == "questionable") {
                m_validity = HnzValidity::QUESTIONABLE;
            }
            else if (validityStr == "reserved") {
                m_validity = HnzValidity::RESERVED;
            }
            else {
                throw HnzPivotObjectException("Validity has invalid value: " + validityStr);
            }
        }
        else if (child->getName() == "Source") {
            std::string sourceStr = getValueStr(child);
            if (sourceStr == "process") {
                continue;
            }

            if (sourceStr == "substituted") {
                m_source = HnzSource::SUBSTITUTED;
            }
            else {
                throw HnzPivotObjectException("Source has invalid value: " + sourceStr);
            }
        }
        else if (child->getName() == "DetailQuality") {
            handleDetailQuality(child);
        }
        else if (readBool(child, "operatorBlocked", m_operatorBlocked)) {
            continue;
        }
        else if (readBool(child, "test", m_test)) {
            continue;
        }
    }
}

void HnzPivotObject::handleCdcSps(Datapoint* cdc) {
    auto sps = getChild(cdc, "ctlVal");
    if (sps == nullptr) {
        sps = getChild(cdc, "stVal");
        if (sps == nullptr) {
            intVal = 0;
            return;
        }
    }
    intVal = getValueInt(sps);
}

void HnzPivotObject::handleCdcDps(Datapoint* dps) {
    auto sps = getChild(dps, "ctlVal");
    if (sps == nullptr) {
        sps = getChild(dps, "stVal");
        if (sps == nullptr) {
            intVal = 0;
            return;
        }
    }
    intVal = getValueStr(sps) == "on";
}



void HnzPivotObject::handleCdcMv(Datapoint* cdc) {
    std::string beforeLog = HNZPivotConfig::getPluginName() + " - HnzPivotObject::handleCdcMv -";
    Datapoint *magVal = getChild(cdc, "mag");
    if (magVal == nullptr) {
        throw HnzPivotObjectException("No mag key for type MvTyp");
    }
    auto magI = getChild(magVal, "i");
    if (magI == nullptr) {
        auto magF = getChild(magVal, "f");
        if(magF == nullptr) {
            throw HnzPivotObjectException("No mag i or mag f for type MvTyp");   
        }

        try
        {
            intVal = static_cast<long>(getValueFloat(magF));
        }
        catch(const HnzPivotObjectException& e)
        {
            HnzPivotUtility::log_warn("%s %s ",beforeLog.c_str(), e.what());
            intVal = getValueInt(magF);
        }
    }
    else {
        intVal = getValueInt(magI);
    }

}

void HnzPivotObject::handleCdcSpc(Datapoint* cdc) {
    const auto ctlVal = getChild(cdc, "ctlVal");

    if (ctlVal) {
        // In Pivot, ON=1, OFF=0
        // In HNZ,   ON=1, OFF=2
        if (getValueInt(ctlVal) > 0) {
            intVal = 1;
        }
        else {
            intVal = 2;
        }
    }
}

void HnzPivotObject::handleCdcDpc(Datapoint* cdc) {
    Datapoint* ctlVal = getChild(cdc, "ctlVal");

    if (ctlVal) {
        // In Pivot, ON="on", OFF="off"
        // In HNZ,   ON=1, OFF=2

        std::string ctlValStr = getValueStr(ctlVal);
        if (ctlValStr == "off") {
            intVal = 2;
        }
        else if (ctlValStr == "on") {
            intVal = 1;
        }
        else {
            throw HnzPivotObjectException("invalid DpcTyp value : " + ctlValStr);
        }
    }
}

void HnzPivotObject::handleCdcT(Datapoint* cdc) {
    const auto t = getChild(cdc, "t");

    if (t) {
        m_timestamp = std::make_shared<HnzPivotTimestamp>(t);
    }
    else {
        HnzPivotUtility::log_debug("No time");
    }
}

void HnzPivotObject::handleCdcQ(Datapoint* cdc) {
    const auto q = getChild(cdc, "q");

    if (q) {
        handleQuality(q);
    } else {
        HnzPivotUtility::log_debug("No quality");
    }
}

void HnzPivotObject::handleCdcInc(Datapoint* cdc) {
    const auto ctlVal = getChild(cdc, "ctlVal");

    if (ctlVal) {
        intVal = getValueLong(ctlVal);
    }
}


void HnzPivotObject::handleCdc(Datapoint* cdc) {
    if (cdc == nullptr) {
        throw HnzPivotObjectException("CDC element not found");
    }
    

    handleCdcQ(cdc);
    handleCdcT(cdc);

    if (m_pivotCdc == HnzPivotCdc::SPS) {
       handleCdcSps(cdc);
    }
    else if (m_pivotCdc == HnzPivotCdc::DPS) {
        handleCdcDps(cdc);
    }
    else if (m_pivotCdc == HnzPivotCdc::MV) {
        handleCdcMv(cdc);
    }
    else if (m_pivotCdc == HnzPivotCdc::SPC)
    {
        handleCdcSpc(cdc);
    }
    else if (m_pivotCdc == HnzPivotCdc::DPC) {
        handleCdcDpc(cdc);
    }
    else if (m_pivotCdc == HnzPivotCdc::INC) {
        handleCdcInc(cdc);
    }
}

void HnzPivotObject::handleGTIX() {
    m_identifier = getChildValueStr(m_ln, "Identifier");

    if (getChild(m_ln, "ComingFrom")) {
        m_comingFrom = getChildValueStr(m_ln, "ComingFrom");
    }

    Datapoint* cause = getChild(m_ln, "Cause");

    if (cause) {
        m_cause = getChildValueInt(cause, "stVal");
    }

    Datapoint* confirmation = getChild(m_ln, "Confirmation");

    if (confirmation) {
        int confirmationVal = getChildValueInt(confirmation, "stVal");

        if (confirmationVal > 0) {
            m_isConfirmation = true;
        }
    }

    Datapoint* tmOrg = getChild(m_ln, "TmOrg");

    if (tmOrg) {
        std::string tmOrgValue = getChildValueStr(tmOrg, "stVal");

        if (tmOrgValue == "substituted") {
            m_timestampSubstituted = true;
        }
        else {
            m_timestampSubstituted = false;
        }
    }

    Datapoint* tmValidity  = getChild(m_ln, "TmValidity");

    if (tmValidity) {
        std::string tmValidityValue = getChildValueStr(tmValidity, "stVal");

        if (tmValidityValue == "invalid") {
            m_timestampInvalid = true;
        }
        else {
            m_timestampInvalid = false;
        }
    }

    Datapoint* cdc = getCdc(m_ln);
    handleCdc(cdc);
}

HnzPivotObject::HnzPivotObject(Datapoint* pivotData) {
    if (pivotData->getName() != "PIVOT") {
        throw HnzPivotObjectException("No pivot object");
    }

    m_dp = pivotData;
    m_ln = nullptr;
    std::vector<std::string> unknownChildrenNames;

    DatapointValue& dpv = pivotData->getData();
    if (dpv.getType() != DatapointValue::T_DP_DICT) {
        throw HnzPivotObjectException("pivot object not found");
    }

    const std::vector<Datapoint*>* datapoints = dpv.getDpVec();
    for (Datapoint* child : *datapoints) {
        if (child->getName() == "GTIS") {
            m_pivotClass = HnzPivotClass::GTIS;
            m_ln = child;
        }
        else if (child->getName() == "GTIM") {
            m_pivotClass = HnzPivotClass::GTIM;
            m_ln = child;
        }
        else if (child->getName() == "GTIC") {
            m_pivotClass = HnzPivotClass::GTIC;
            m_ln = child;
        }
        else {
            unknownChildrenNames.push_back(child->getName());
        }

        if (m_ln != nullptr) {
            break;
        }
    }

    if (m_ln == nullptr) {
        throw HnzPivotObjectException("pivot object type not supported: " + HnzPivotUtility::join(unknownChildrenNames));
    }

    handleGTIX();
}

HnzPivotObject::HnzPivotObject(const std::string& pivotLN, const std::string& valueType)
{
    m_dp = createDp("PIVOT");

    m_ln = addElement(m_dp, pivotLN);

    addElementWithValue(m_ln, "ComingFrom", "hnzip");

    m_cdc = addElement(m_ln, valueType);
}

void HnzPivotObject::setIdentifier(const std::string& identifier)
{
    addElementWithValue(m_ln, "Identifier", identifier);
}

void HnzPivotObject::setCause(int cause)
{
    Datapoint* causeDp = addElement(m_ln, "Cause");

    addElementWithValue(causeDp, "stVal", static_cast<long>(cause));
}

void HnzPivotObject::setStVal(bool value)
{
    addElementWithValue(m_cdc, "stVal", static_cast<long>(value ? 1 : 0));
}

void HnzPivotObject::setStValStr(const std::string& value)
{
    addElementWithValue(m_cdc, "stVal", value);
}

void HnzPivotObject::setMagF(float value)
{
    Datapoint* mag = addElement(m_cdc, "mag");

    addElementWithValue(mag, "f", value);
}

void HnzPivotObject::setMagI(int value)
{
    Datapoint* mag = addElement(m_cdc, "mag");

    addElementWithValue(mag, "i", static_cast<long>(value));
}

void HnzPivotObject::setConfirmation(bool value)
{
    Datapoint* confirmation = addElement(m_ln, "Confirmation");

    if (confirmation) {
        addElementWithValue(confirmation, "stVal", static_cast<long>(value ? 1 : 0));
    }
}

void HnzPivotObject::addQuality(unsigned int doValid, bool doOutdated, bool doTsC, bool doTsS)
{
    Datapoint* q = addElement(m_cdc, "q");
    // doValid of 1 means "invalid"
    if (doValid == 1) {
        addElementWithValue(q, "Validity", "invalid");
    }
    else if (doOutdated || doTsC || doTsS) {
        addElementWithValue(q, "Validity", "questionable");
    }
    else {
        addElementWithValue(q, "Validity", "good");
    }

    if (doTsC || doOutdated) {
        Datapoint* detailQuality = addElement(q, "DetailQuality");

        addElementWithValue(detailQuality, "oldData", 1L);
    }
}

void HnzPivotObject::addTmOrg(bool substituted)
{
    Datapoint* tmOrg = addElement(m_ln, "TmOrg");

    if (substituted)
        addElementWithValue(tmOrg, "stVal", "substituted");
    else
        addElementWithValue(tmOrg, "stVal", "genuine");
}

void HnzPivotObject::addTmValidity(bool invalid)
{
    Datapoint* tmValidity = addElement(m_ln, "TmValidity");

    if (invalid)
        addElementWithValue(tmValidity, "stVal", "invalid");
    else
        addElementWithValue(tmValidity, "stVal", "good");
}

HnzPivotObject::HnzPivotClass HnzPivotObject::getPivotClass() const
{
    return m_pivotClass;
}

void HnzPivotObject::addTimestamp(unsigned long doTs, bool doTsS)
{
    Datapoint* t = addElement(m_cdc, "t");

    auto timePair = HnzPivotTimestamp::fromTimestamp(static_cast<long>(doTs));
    addElementWithValue(t, "SecondSinceEpoch", timePair.first);
    addElementWithValue(t, "FractionOfSecond", timePair.second);

    if (doTsS) {
        Datapoint* timeQuality = addElement(t, "TimeQuality");
        addElementWithValue(timeQuality, "clockNotSynchronized", 1L);
    }
}


std::vector<Datapoint *> HnzPivotObject::toHnzTMObject(std::shared_ptr<HNZPivotDataPoint> exchangeConfig) const {
    std::vector<Datapoint *> hnzObject;

    Datapoint *type = createDpWithValue("do_type", exchangeConfig->getTypeId());
    hnzObject.push_back(type);

    Datapoint *station = createDpWithValue("do_station", static_cast<long>(exchangeConfig->getStation()));
    hnzObject.push_back(station);

    Datapoint *addr = createDpWithValue("do_addr", static_cast<long>(exchangeConfig->getAddress()));
    hnzObject.push_back(addr);

    if (intVal >= -127 && intVal <= 127)
    {
        Datapoint *an = createDpWithValue("do_an", "TMA");
        hnzObject.push_back(an);
    }
    else if (intVal >= 128 && intVal <= 255)
    {
        Datapoint *an = createDpWithValue("do_an", "TM8");
        hnzObject.push_back(an);
    }
    else
    {
        Datapoint *an = createDpWithValue("do_an", "TM16");
        hnzObject.push_back(an);
    }

    Datapoint *value = createDpWithValue("do_value", intVal);
    hnzObject.push_back(value);

    toHnzValidityObject(hnzObject);

    return hnzObject;
}

void HnzPivotObject::toHnzValidityObject(std::vector<Datapoint *> &dps) const
{
    if (m_validity == HnzValidity::INVALID)
    {
        Datapoint *valid = createDpWithValue("do_valid", 1L);
        dps.push_back(valid);
        Datapoint *outdated = createDpWithValue("do_outdated", 1L);
        dps.push_back(outdated);
    }
    else if (m_validity == HnzValidity::QUESTIONABLE)
    {
        Datapoint *valid = createDpWithValue("do_valid", 0L);
        dps.push_back(valid);
        Datapoint *outdated = createDpWithValue("do_outdated", 1L);
        dps.push_back(outdated);
    }
    else
    {
        Datapoint *valid = createDpWithValue("do_valid", 0L);
        dps.push_back(valid);
        Datapoint *outdated = createDpWithValue("do_outdated", 0L);
        dps.push_back(outdated);
    }

}

std::vector<Datapoint *> HnzPivotObject::toHnzTSObject(std::shared_ptr<HNZPivotDataPoint> exchangeConfig) const
{
    std::vector<Datapoint *> hnzObject;

    Datapoint *type = createDpWithValue("do_type", exchangeConfig->getTypeId());
    hnzObject.push_back(type);

    Datapoint *station = createDpWithValue("do_station", static_cast<long>(exchangeConfig->getStation()));
    hnzObject.push_back(station);

    Datapoint *addr = createDpWithValue("do_addr", static_cast<long>(exchangeConfig->getAddress()));
    hnzObject.push_back(addr);

    // TSCE
    if (m_cause == 3)
    {
        Datapoint *cg = createDpWithValue("do_cg", 0L);
        hnzObject.push_back(cg);

        Datapoint *val = createDpWithValue("do_value", intVal);
        hnzObject.push_back(val);

        Datapoint *ts = createDpWithValue("do_ts", 
        HnzPivotTimestamp::toTimestamp(m_timestamp->SecondSinceEpoch(),
        m_timestamp->FractionOfSecond()));
        hnzObject.push_back(ts);

        Datapoint *tm_validity = getChild(m_ln, "TmValidity");
        std::string tm_validity_value = "good";
        if (tm_validity)
        {
            tm_validity_value = getChildValueStr(tm_validity, "stVal");
        }

        if(tm_validity_value == "good") {
            Datapoint *ts_s = createDpWithValue("do_ts_iv", 0L);
            hnzObject.push_back(ts_s);
        } else {
            Datapoint *ts_s = createDpWithValue("do_ts_iv", 1L);
            hnzObject.push_back(ts_s);
        }

        if(m_validity == HnzValidity::QUESTIONABLE || m_validity == HnzValidity::GOOD) {
            Datapoint *ts_s_c = createDpWithValue("do_ts_c", 0L);
            hnzObject.push_back(ts_s_c);

            Datapoint *ts_s = createDpWithValue("do_ts_s", 0L);
            hnzObject.push_back(ts_s);

        } else {
            Datapoint *ts_s_c = createDpWithValue("do_ts_c", 1L);
            hnzObject.push_back(ts_s_c);

            Datapoint *ts_s = createDpWithValue("do_ts_s", 1L);
            hnzObject.push_back(ts_s);
        }

    // TS CG
    }
    else if (m_cause == 20)
    {
        Datapoint *cg = createDpWithValue("do_cg", 1L);
        hnzObject.push_back(cg);

        Datapoint *val = createDpWithValue("do_value", intVal);
        hnzObject.push_back(val);
    }

    toHnzValidityObject(hnzObject);

    return hnzObject;
}

std::vector<Datapoint *> HnzPivotObject::toHnzObject(std::shared_ptr<HNZPivotDataPoint> exchangeConfig) const
{
    // TM
    if (m_pivotCdc == HnzPivotCdc::MV)
    {
        return toHnzTMObject(exchangeConfig);
    }
    // TS simple or double
    else if (m_pivotCdc == HnzPivotCdc::SPS || m_pivotCdc == HnzPivotCdc::DPS)
    {
        return toHnzTSObject(exchangeConfig);
    }

    return {};
}

std::vector<Datapoint *> HnzPivotObject::toHnzCommandObject(std::shared_ptr<HNZPivotDataPoint> exchangeConfig) const
{
    std::vector<Datapoint*> commandObject;

    Datapoint* type = createDpWithValue("co_type", exchangeConfig->getTypeId());
    commandObject.push_back(type);

    Datapoint* addr = createDpWithValue("co_addr", static_cast<long>(exchangeConfig->getAddress()));
    commandObject.push_back(addr);

    Datapoint* value = createDpWithValue("co_value", intVal);
    commandObject.push_back(value);

    return commandObject;
}

bool HnzPivotObject::checkCdcTypeMatch(HnzPivotCdc pivotCdc, HnzPivotClass pivotClass) {
    switch (pivotClass) {
        case HnzPivotClass::GTIS:
            return pivotCdc == HnzPivotCdc::SPS || pivotCdc == HnzPivotCdc::DPS;
        case HnzPivotClass::GTIM:
            return pivotCdc == HnzPivotCdc::MV;
        case HnzPivotClass::GTIC:
            return pivotCdc == HnzPivotCdc::SPC || pivotCdc == HnzPivotCdc::DPC || pivotCdc == HnzPivotCdc::INC;
    }
    return false;
}

std::string HnzPivotObject::HnzPivotCdcStr(HnzPivotCdc pivotCdc) {
    switch (pivotCdc) {
        case HnzPivotCdc::SPS:
            return "SpsTyp";
        case HnzPivotCdc::DPS:
            return "DpsTyp";
        case HnzPivotCdc::MV:
            return "MvTyp";
        case HnzPivotCdc::SPC:
            return "SpcTyp";
        case HnzPivotCdc::DPC:
            return "DpcTyp";
        case HnzPivotCdc::INC:
            return "IncTyp";
    }
    return "";
}

std::string HnzPivotObject::HnzPivotClassStr(HnzPivotClass pivotClass) {
    switch (pivotClass) {
        case HnzPivotClass::GTIS:
            return "GTIS";
        case HnzPivotClass::GTIM:
            return "GTIM";
        case HnzPivotClass::GTIC:
            return "GTIC";
    }
    return "";
}
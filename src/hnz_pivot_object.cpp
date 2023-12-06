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

    DatapointValue& dpv = dp->getData();

    if (dpv.getType() == DatapointValue::T_DP_DICT) {
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
        throw PivotObjectException("datapoint " + dp->getName() + " has not a string value");
    }
}

static std::string getChildValueStr(Datapoint* dp, const std::string& name)
{
    Datapoint* childDp = getChild(dp, name);

    if (childDp) {
        return getValueStr(childDp);
    }
    else {
        throw PivotObjectException("No such child: " + name);
    }
}

static long getValueLong(Datapoint* dp)
{
    const DatapointValue& dpv = dp->getData();

    if (dpv.getType() == DatapointValue::T_INTEGER) {
        return dpv.toInt();
    }
    else {
        throw PivotObjectException("datapoint " + dp->getName() + " has not an int value");
    }
}

static long getChildValueLong(Datapoint* dp, const std::string& name)
{
    Datapoint* childDp = getChild(dp, name);

    if (childDp) {
        return getValueLong(childDp);
    }
    else {
        throw PivotObjectException("No such child: " + name);
    }
}

static int getValueInt(Datapoint* dp)
{
    return static_cast<int>(getValueLong(dp));
}

static int getChildValueInt(Datapoint* dp, const std::string& name)
{
    return static_cast<int>(getChildValueLong(dp, name));
}

void PivotTimestamp::handleTimeQuality(Datapoint* timeQuality)
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

PivotTimestamp::PivotTimestamp(Datapoint* timestampData)
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

long PivotTimestamp::toTimestamp(long secondSinceEpoch, long fractionOfSecond) {
    long timestamp = 0;
    auto msPart = static_cast<long>(round(static_cast<double>(fractionOfSecond * 1000) / 16777216.0));
    timestamp = (secondSinceEpoch * 1000L) + msPart;
    return timestamp;
}

std::pair<long, long> PivotTimestamp::fromTimestamp(long timestamp) {
    long remainder = (timestamp % 1000L);
    long fractionOfSecond = remainder * 16777 + ((remainder * 216) / 1000);
    return std::make_pair(timestamp / 1000L, fractionOfSecond);
}

uint64_t PivotTimestamp::getCurrentTimestampMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

Datapoint* PivotObject::getCdc(Datapoint* dp)
{
    Datapoint* cdcDp = nullptr;
    std::vector<std::string> unknownChildrenNames;

    DatapointValue& dpv = dp->getData();
    if (dpv.getType() != DatapointValue::T_DP_DICT) {
        throw PivotObjectException("CDC type missing");
    }

    const std::vector<Datapoint*>* datapoints = dpv.getDpVec();
    for (Datapoint* child : *datapoints) {
        if (child->getName() == "SpsTyp") {
            cdcDp = child;
            m_pivotCdc = PivotCdc::SPS;
        }
        else if (child->getName() == "MvTyp") {
            cdcDp = child;
            m_pivotCdc = PivotCdc::MV;
        }
        else if (child->getName() == "DpsTyp") {
            cdcDp = child;
            m_pivotCdc = PivotCdc::DPS;
        }
        else if (child->getName() == "SpcTyp") {
            cdcDp = child;
            m_pivotCdc = PivotCdc::SPC;
        }
        else if (child->getName() == "DpcTyp") {
            cdcDp = child;
            m_pivotCdc = PivotCdc::DPC;
        }
        else if (child->getName() == "IncTyp") {
            cdcDp = child;
            m_pivotCdc = PivotCdc::INC;
        }
        else {
            unknownChildrenNames.push_back(child->getName());
        }
        if (cdcDp != nullptr) {
            break;
        }
    }
    if(cdcDp == nullptr) {
        throw PivotObjectException("CDC type unknown: " + PivotUtility::join(unknownChildrenNames));
    }
    
    return cdcDp;
}

bool PivotObject::readBool(Datapoint* dp, const std::string& name, bool& out) const
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

void PivotObject::handleDetailQuality(Datapoint* detailQuality)
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

void PivotObject::handleQuality(Datapoint* q)
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
                m_validity = Validity::INVALID;
            }
            else if (validityStr == "questionable") {
                m_validity = Validity::QUESTIONABLE;
            }
            else if (validityStr == "reserved") {
                m_validity = Validity::RESERVED;
            }
            else {
                throw PivotObjectException("Validity has invalid value: " + validityStr);
            }
        }
        else if (child->getName() == "Source") {
            std::string sourceStr = getValueStr(child);
            if (sourceStr == "process") {
                continue;
            }

            if (sourceStr == "substituted") {
                m_source = Source::SUBSTITUTED;
            }
            else {
                throw PivotObjectException("Source has invalid value: " + sourceStr);
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

void PivotObject::handleCdc(Datapoint* cdc) {
    if (cdc == nullptr) {
        throw PivotObjectException("CDC element not found");
    }
    
    Datapoint* q = getChild(cdc, "q");

    if (q) {
        handleQuality(q);
    }

    Datapoint* t = getChild(cdc, "t");

    if (t) {
        m_timestamp = std::make_shared<PivotTimestamp>(t);
    }

    if (m_pivotCdc == PivotCdc::SPS) {
        throw PivotObjectException("Pivot to HNZ not implemented for type SpsTyp");
    }
    else if (m_pivotCdc == PivotCdc::DPS) {
        throw PivotObjectException("Pivot to HNZ not implemented for type DpsTyp");
    }
    else if (m_pivotCdc == PivotCdc::MV) {
        throw PivotObjectException("Pivot to HNZ not implemented for type MvTyp");
    }
    else if (m_pivotCdc == PivotCdc::SPC) {
        Datapoint* ctlVal = getChild(cdc, "ctlVal");

        if (ctlVal) {
            if (getValueInt(ctlVal) > 0) {
                intVal = 1;
            }
            else {
                intVal = 0;
            }
        }
    }
    else if (m_pivotCdc == PivotCdc::DPC) {
        Datapoint* ctlVal = getChild(cdc, "ctlVal");

        if (ctlVal) {
            std::string ctlValStr = getValueStr(ctlVal);
            if (ctlValStr == "off") {
                intVal = 0;
            }
            else if (ctlValStr == "on") {
                intVal = 1;
            }
            else {
                throw PivotObjectException("invalid DpcTyp value : " + ctlValStr);
            }
        }
    }
    else if (m_pivotCdc == PivotCdc::INC) {
        Datapoint* ctlVal = getChild(cdc, "ctlVal");

        if (ctlVal) {
            intVal = getValueLong(ctlVal);
        }
    }
}

void PivotObject::handleGTIX() {
    m_identifier = getChildValueStr(m_ln, "Identifier");

    if (getChild(m_ln, "ComingFrom")) {
        m_comingFrom = getChildValueStr(m_ln, "ComingFrom");
    }

    Datapoint* cause = getChild(m_ln, "Cause");

    if (cause) {
        m_cause = getChildValueInt(cause, "stVal");
    }

    Datapoint* confirmation = getChild(m_ln, "Cause");

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

PivotObject::PivotObject(Datapoint* pivotData) {
    if (pivotData->getName() != "PIVOT") {
        throw PivotObjectException("No pivot object");
    }

    m_dp = pivotData;
    m_ln = nullptr;
    std::vector<std::string> unknownChildrenNames;

    DatapointValue& dpv = pivotData->getData();
    if (dpv.getType() != DatapointValue::T_DP_DICT) {
        throw PivotObjectException("pivot object not found");
    }

    const std::vector<Datapoint*>* datapoints = dpv.getDpVec();
    for (Datapoint* child : *datapoints) {
        if (child->getName() == "GTIS") {
            m_pivotClass = PivotClass::GTIS;
            m_ln = child;
        }
        else if (child->getName() == "GTIM") {
            m_pivotClass = PivotClass::GTIM;
            m_ln = child;
        }
        else if (child->getName() == "GTIC") {
            m_pivotClass = PivotClass::GTIC;
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
        throw PivotObjectException("pivot object type not supported: " + PivotUtility::join(unknownChildrenNames));
    }

    handleGTIX();
}

PivotObject::PivotObject(const std::string& pivotLN, const std::string& valueType)
{
    m_dp = createDp("PIVOT");

    m_ln = addElement(m_dp, pivotLN);

    addElementWithValue(m_ln, "ComingFrom", "hnzip");

    m_cdc = addElement(m_ln, valueType);
}

void PivotObject::setIdentifier(const std::string& identifier)
{
    addElementWithValue(m_ln, "Identifier", identifier);
}

void PivotObject::setCause(int cause)
{
    Datapoint* causeDp = addElement(m_ln, "Cause");

    addElementWithValue(causeDp, "stVal", static_cast<long>(cause));
}

void PivotObject::setStVal(bool value)
{
    addElementWithValue(m_cdc, "stVal", static_cast<long>(value ? 1 : 0));
}

void PivotObject::setStValStr(const std::string& value)
{
    addElementWithValue(m_cdc, "stVal", value);
}

void PivotObject::setMagF(float value)
{
    Datapoint* mag = addElement(m_cdc, "mag");

    addElementWithValue(mag, "f", value);
}

void PivotObject::setMagI(int value)
{
    Datapoint* mag = addElement(m_cdc, "mag");

    addElementWithValue(mag, "i", static_cast<long>(value));
}

void PivotObject::setConfirmation(bool value)
{
    Datapoint* confirmation = addElement(m_ln, "Confirmation");

    if (confirmation) {
        addElementWithValue(confirmation, "stVal", static_cast<long>(value ? 1 : 0));
    }
}

void PivotObject::addQuality(unsigned int doValid, bool doOutdated, bool doTsC, bool doTsS)
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

void PivotObject::addTmOrg(bool substituted)
{
    Datapoint* tmOrg = addElement(m_ln, "TmOrg");

    if (substituted)
        addElementWithValue(tmOrg, "stVal", "substituted");
    else
        addElementWithValue(tmOrg, "stVal", "genuine");
}

void PivotObject::addTmValidity(bool invalid)
{
    Datapoint* tmValidity = addElement(m_ln, "TmValidity");

    if (invalid)
        addElementWithValue(tmValidity, "stVal", "invalid");
    else
        addElementWithValue(tmValidity, "stVal", "good");
}

void PivotObject::addTimestamp(unsigned long doTs, bool doTsS)
{
    Datapoint* t = addElement(m_cdc, "t");

    auto timePair = PivotTimestamp::fromTimestamp(static_cast<long>(doTs));
    addElementWithValue(t, "SecondSinceEpoch", timePair.first);
    addElementWithValue(t, "FractionOfSecond", timePair.second);

    if (doTsS) {
        Datapoint* timeQuality = addElement(t, "TimeQuality");
        addElementWithValue(timeQuality, "clockNotSynchronized", 1L);
    }
}

std::vector<Datapoint*> PivotObject::toHnzCommandObject(std::shared_ptr<HNZPivotDataPoint> exchangeConfig)
{
    std::vector<Datapoint*> commandObject;

    Datapoint* type = createDpWithValue("co_type", exchangeConfig->getTypeId());
    commandObject.push_back(type);

    Datapoint* ca = createDpWithValue("co_addr", static_cast<long>(exchangeConfig->getAddress()));
    commandObject.push_back(ca);

    Datapoint* ioa = createDpWithValue("co_value", intVal);
    commandObject.push_back(ioa);

    return commandObject;
}
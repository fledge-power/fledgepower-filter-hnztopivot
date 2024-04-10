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

#ifndef _HNZ_PIVOT_OBJECT_H
#define _HNZ_PIVOT_OBJECT_H

#include <string>
#include <memory>

class Datapoint;
class HNZPivotDataPoint;

class HnzPivotObjectException : public std::exception //NOSONAR
{     
 public:
    explicit HnzPivotObjectException(const std::string& context):
        m_context(context) {}

    const std::string& getContext(void) const {return m_context;}

 private:
    const std::string m_context;
};

class HnzPivotTimestamp
{
public:
    explicit HnzPivotTimestamp(Datapoint* timestampData);
    HnzPivotTimestamp(HnzPivotTimestamp& other) = delete;
    HnzPivotTimestamp& operator=(const HnzPivotTimestamp& other) = delete;
    
    int SecondSinceEpoch() const {return m_secondSinceEpoch;}
    int FractionOfSecond() const {return m_fractionOfSecond;}
    
    bool ClockFailure() const {return m_clockFailure;}
    bool LeapSecondKnown() const {return m_leapSecondKnown;}
    bool ClockNotSynchronized() const {return m_clockNotSynchronized;}
    int TimeAccuracy() const {return m_timeAccuracy;}

    /**
     * Convert secondSinceEpoch and secondSinceEpoch to timestamp
     * @param secondSinceEpoch : interval in seconds continuously counted from the epoch 1970-01-01 00:00:00 UTC
     * @param fractionOfSecond : represents the fraction of the current second when the value of the TimeStamp has been determined.
     * @return timestamp (ms)
    */
    static long toTimestamp(long secondSinceEpoch, long fractionOfSecond);

    /**
     * Convert timestamp (ms) in pair of secondSinceEpoch and fractionOfSecond
     * @param timestamp : timestamp (ms) 
     * @return pair of secondSinceEpoch and fractionOfSecond
    */
    static std::pair<long, long> fromTimestamp(long timestamp);

    /**
     * Get current timestamp in milisecond
     * @return timestamp in ms
    */
    static uint64_t getCurrentTimestampMs();

private:

    void handleTimeQuality(Datapoint* timeQuality);

    int m_secondSinceEpoch{0};
    int m_fractionOfSecond{0};
    
    int m_timeAccuracy{0};
    bool m_clockFailure{false};
    bool m_leapSecondKnown{false};
    bool m_clockNotSynchronized{false};
};

class HnzPivotObject
{
public:

    enum class HnzPivotClass
	{
		GTIS,
		GTIM,
        GTIC
	};

    enum class HnzPivotCdc
    {
        SPS,
        DPS,
        MV,
        SPC,
        DPC,
        INC
    };

    enum class HnzValidity
    {
        GOOD,
        INVALID,
        RESERVED,
        QUESTIONABLE
    };

    enum class HnzSource
    {
        PROCESS,
        SUBSTITUTED
    };

    explicit HnzPivotObject(Datapoint* pivotData);
    HnzPivotObject(const std::string& pivotLN, const std::string& valueType);

    void setIdentifier(const std::string& identifier);
    void setCause(int cause);

    void setStVal(bool value);
    void setStValStr(const std::string& value);

    void setMagF(float value);
    void setMagI(int value);

    void setConfirmation(bool value);

    void addTimestamp(unsigned long doTs, bool doTsS);



    void addQuality(unsigned int doValid, bool doOutdated, bool doTsC, bool doTsS);
    void addTmOrg(bool substituted);
    void addTmValidity(bool invalid);

    HnzPivotClass getPivotClass() const;

    Datapoint* toDatapoint() {return m_dp;}

    std::vector<Datapoint *> toHnzTMObject(std::shared_ptr<HNZPivotDataPoint> exchangeConfig) const;
    void toHnzValidityObject(std::vector<Datapoint *> &dps) const;
    std::vector<Datapoint *> toHnzTSObject(std::shared_ptr<HNZPivotDataPoint> exchangeConfig) const;
    std::vector<Datapoint *> toHnzObject(std::shared_ptr<HNZPivotDataPoint> exchangeConfig) const;
    std::vector<Datapoint*> toHnzCommandObject(std::shared_ptr<HNZPivotDataPoint> exchangeConfig) const;

    const std::string& getIdentifier() const {return m_identifier;}
    const std::string& getComingFrom() const {return m_comingFrom;}
    int getCause() const {return m_cause;}
    bool isConfirmation() const {return m_isConfirmation;}

    HnzValidity getValidity() const {return m_validity;}
    HnzSource getSource() const {return m_source;}

    bool BadReference() const {return m_badReference;}
    bool Failure() const {return m_failure;}
    bool Inconsistent() const {return m_inconsistent;}
    bool OldData() const {return m_oldData;}
    bool Oscillatory() const {return m_oscillatory;}
    bool OutOfRange() const {return m_outOfRange;}
    bool Overflow() const {return m_overflow;}

    bool OperatorBlocked() const {return m_operatorBlocked;}
    bool Test() const {return m_test;}

    bool IsTimestampSubstituted() const {return m_timestampSubstituted;}
    bool IsTimestampInvalid() const {return m_timestampInvalid;}

    static bool checkCdcTypeMatch(HnzPivotCdc pivotCdc, HnzPivotClass pivotClass);
    static std::string HnzPivotCdcStr(HnzPivotCdc pivotCdc);
    static std::string HnzPivotClassStr(HnzPivotClass pivotClass);

private:
    Datapoint* getCdc(Datapoint* dp);
    bool readBool(Datapoint* dp, const std::string& name, bool& out) const;
    void handleGTIX();
    void handleCdc(Datapoint* cdc);
    void handleDetailQuality(Datapoint* detailQuality);
    void handleQuality(Datapoint* q);

    Datapoint* m_dp;
    Datapoint* m_ln;
    Datapoint* m_cdc;
    HnzPivotClass m_pivotClass;
    HnzPivotCdc m_pivotCdc;

    std::string m_comingFrom;
    std::string m_identifier;
    int m_cause = 0;
    bool m_isConfirmation = false;

    HnzValidity m_validity = HnzValidity::GOOD;
    bool m_badReference = false;
    bool m_failure = false;
    bool m_inconsistent = false;
    bool m_inacurate = false;
    bool m_oldData = false;
    bool m_oscillatory = false;
    bool m_outOfRange = false;
    bool m_overflow = false;
    HnzSource m_source = HnzSource::PROCESS;
    bool m_operatorBlocked = false;
    bool m_test = false;

    std::shared_ptr<HnzPivotTimestamp> m_timestamp;

    bool m_timestampSubstituted = false;
    bool m_timestampInvalid = false;

    long intVal = 0;
    
};

#endif /* _HNZ_PIVOT_OBJECT_H */
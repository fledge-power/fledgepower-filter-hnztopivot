#include <gtest/gtest.h>
#include <plugin_api.h>
#include <reading.h>
#include <reading_set.h>
#include <filter.h>
#include <rapidjson/document.h>
#include "hnz_pivot_filter.hpp"
#include "hnz_pivot_utility.hpp"
#include "hnz_pivot_object.hpp"

using namespace rapidjson;

extern "C" {
	PLUGIN_INFORMATION *plugin_info();

    PLUGIN_HANDLE plugin_init(ConfigCategory* config,
                          OUTPUT_HANDLE *outHandle,
                          OUTPUT_STREAM output);

    void plugin_shutdown(PLUGIN_HANDLE handle);
    void plugin_reconfigure(PLUGIN_HANDLE *handle, const std::string& newConfig);
    void plugin_ingest(PLUGIN_HANDLE handle, READINGSET *readingSet);
};

static const std::string test_config = QUOTE({
    "enable" :{
        "value": "true"
    },
    "exchanged_data": {
        "value" : {
            "exchanged_data": {
                "name" : "SAMPLE",
                "version" : "1.0",
                "datapoints" : [
                    {
                        "label" : "TS1",
                        "pivot_id" : "ID114561",
                        "pivot_type" : "SpsTyp",
                        "protocols" : [
                            {
                                "name" : "hnzip",
                                "address" : "511",
                                "typeid" : "TS"
                            }
                        ]
                    },
                    {
                        "label" : "TS2",
                        "pivot_id" : "ID114562",
                        "pivot_type" : "DpsTyp",
                        "protocols" : [
                            {
                                "name" : "hnzip",
                                "address" : "522",
                                "typeid" : "TS"
                            }
                        ]
                    },
                    {
                        "label" : "TS3",
                        "pivot_id" : "ID114567",
                        "pivot_type" : "SpsTyp",
                        "protocols" : [
                            {
                                "name" : "hnzip",
                                "address" : "577",
                                "typeid" : "TS"
                            }
                        ]
                    },
                    {
                        "label" : "TM1",
                        "pivot_id" : "ID111111",
                        "pivot_type" : "MvTyp",
                        "protocols" : [
                            {
                                "name" : "hnzip",
                                "address" : "20",
                                "typeid" : "TM"
                            }
                        ]
                    },
                    {
                        "label" : "TM2",
                        "pivot_id" : "ID111222",
                        "pivot_type" : "MvTyp",
                        "protocols" : [
                            {
                                "name" : "hnzip",
                                "address" : "21",
                                "typeid" : "TM"
                            }
                        ]
                    },
                    {
                        "label" : "TM3",
                        "pivot_id" : "ID111333",
                        "pivot_type" : "MvTyp",
                        "protocols" : [
                            {
                                "name" : "hnzip",
                                "address" : "22",
                                "typeid" : "TM"
                            }
                        ]
                    },
                    {
                        "label" : "TM4",
                        "pivot_id" : "ID111444",
                        "pivot_type" : "MvTyp",
                        "protocols" : [
                            {
                                "name" : "hnzip",
                                "address" : "23",
                                "typeid" : "TM"
                            }
                        ]
                    },
                    {
                        "label" : "TC1",
                        "pivot_id" : "ID222222",
                        "pivot_type" : "SpcTyp",
                        "protocols" : [
                            {
                                "name": "hnzip",
                                "address" : "142",
                                "typeid" : "TC"
                            }
                        ]
                    },
                    {
                        "label" : "TC2",
                        "pivot_id" : "ID333333",
                        "pivot_type" : "DpcTyp",
                        "protocols" : [
                            {
                                "name": "hnzip",
                                "address" : "143",
                                "typeid" : "TC"
                            }
                        ]
                    },
                    {
                        "label" : "TVC1",
                        "pivot_id" : "ID444444",
                        "pivot_type" : "IncTyp",
                        "protocols" : [
                            {
                                "name": "hnzip",
                                "address" : "31",
                                "typeid" : "TVC"
                            }
                        ]
                    }
                ]
            }
        }
    }
});

static int outputHandlerCalled = 0;
static std::shared_ptr<Reading> lastReading = nullptr;
// Dummy object used to be able to call parseJson() freely
static DatapointValue dummyValue("");
static Datapoint dummyDataPoint({}, dummyValue);

static const std::vector<std::string> allCommandAttributeNames = {
    "co_type", "co_addr", "co_value"
};
static const std::vector<std::string> allPivotAttributeNames = {
    // TS messages
    "GTIS.ComingFrom", "GTIS.Identifier", "GTIS.Cause.stVal", "GTIS.TmValidity.stVal", "GTIS.TmOrg.stVal",
    "GTIS.SpsTyp.stVal", "GTIS.SpsTyp.q.Validity", "GTIS.SpsTyp.q.DetailQuality.oldData",
    "GTIS.SpsTyp.t.SecondSinceEpoch", "GTIS.SpsTyp.t.FractionOfSecond", "GTIS.SpsTyp.t.TimeQuality.clockNotSynchronized",
    "GTIS.DpsTyp.stVal", "GTIS.DpsTyp.q.Validity", "GTIS.DpsTyp.q.DetailQuality.oldData",
    "GTIS.DpsTyp.t.SecondSinceEpoch", "GTIS.DpsTyp.t.FractionOfSecond", "GTIS.DpsTyp.t.TimeQuality.clockNotSynchronized",
    // TM messages
    "GTIM.ComingFrom", "GTIM.Identifier", "GTIM.Cause.stVal", "GTIM.TmValidity.stVal", "GTIM.TmOrg.stVal",
    "GTIM.MvTyp.mag.i", "GTIM.MvTyp.q.Validity", "GTIM.MvTyp.q.DetailQuality.oldData",
    "GTIM.MvTyp.t.SecondSinceEpoch", "GTIM.MvTyp.t.FractionOfSecond", "GTIM.MvTyp.t.TimeQuality.clockNotSynchronized",
    // TC/TVC messages
    "GTIC.ComingFrom", "GTIC.Identifier", "GTIC.Cause.stVal", "GTIC.TmValidity.stVal", "GTIC.TmOrg.stVal",
    "GTIC.Confirmation.stVal",
    "GTIC.SpcTyp.stVal", "GTIC.SpcTyp.ctlVal", "GTIC.SpcTyp.q.Validity", "GTIC.SpcTyp.q.DetailQuality.oldData",
    "GTIC.SpcTyp.t.SecondSinceEpoch", "GTIC.SpcTyp.t.FractionOfSecond", "GTIC.SpcTyp.t.TimeQuality.clockNotSynchronized",
    "GTIC.DpcTyp.stVal", "GTIC.DpcTyp.ctlVal", "GTIC.DpcTyp.q.Validity", "GTIC.DpcTyp.q.DetailQuality.oldData",
    "GTIC.DpcTyp.t.SecondSinceEpoch", "GTIC.DpcTyp.t.FractionOfSecond", "GTIC.DpcTyp.t.TimeQuality.clockNotSynchronized",
    "GTIC.IncTyp.stVal", "GTIC.IncTyp.ctlVal", "GTIC.IncTyp.q.Validity", "GTIC.IncTyp.q.DetailQuality.oldData",
    "GTIC.IncTyp.t.SecondSinceEpoch", "GTIC.IncTyp.t.FractionOfSecond", "GTIC.IncTyp.t.TimeQuality.clockNotSynchronized",
};
static const std::vector<std::string> allSouthEventAttributeNames = {
    "connx_status", "gi_status"
};

static void createReadingSet(ReadingSet*& outReadingSet, const std::string& assetName, const std::vector<std::string>& jsons)
{
    std::vector<Datapoint*> *allPoints = new std::vector<Datapoint*>();
    for(const std::string& json: jsons) {
        // Create Reading
        std::vector<Datapoint*> *p = nullptr;
        ASSERT_NO_THROW(p = dummyDataPoint.parseJson(json));
        ASSERT_NE(p, nullptr);
        allPoints->insert(std::end(*allPoints), std::begin(*p), std::end(*p));
        delete p;
    }
    Reading *reading = new Reading(assetName, *allPoints);
    std::vector<Reading*> *readings = new std::vector<Reading*>();
    readings->push_back(reading);
    // Create ReadingSet
    outReadingSet = new ReadingSet(readings);
}

static void createReadingSet(ReadingSet*& outReadingSet, const std::string& assetName, const std::string& json)
{
    createReadingSet(outReadingSet, assetName, std::vector<std::string>{json});
}

static void createEmptyReadingSet(ReadingSet*& outReadingSet, const std::string& assetName)
{
    std::vector<Datapoint*> *allPoints = new std::vector<Datapoint*>();
    Reading *reading = new Reading(assetName, *allPoints);
    std::vector<Reading*> *readings = new std::vector<Reading*>();
    readings->push_back(reading);
    // Create ReadingSet
    outReadingSet = new ReadingSet(readings);
}

static bool hasChild(Datapoint& dp, const std::string& childLabel) {
    DatapointValue& dpv = dp.getData();

    const std::vector<Datapoint*>* dps = dpv.getDpVec();

    for (auto sdp : *dps) {
        if (sdp->getName() == childLabel) {
            return true;
        }
    }

    return false;
}

static Datapoint* getChild(Datapoint& dp, const std::string& childLabel) {
    DatapointValue& dpv = dp.getData();

    const std::vector<Datapoint*>* dps = dpv.getDpVec();

    for (Datapoint* childDp : *dps) {
        if (childDp->getName() == childLabel) {
            return childDp;
        }
    }

    return nullptr;
}

template <typename T>
static T callOnLastPathElement(Datapoint& dp, const std::string& childPath, std::function<T(Datapoint&, const std::string&)> func) {
    if (childPath.find(".") != std::string::npos) {
        // Check if the first element in the path is a child of current datapoint
        std::vector<std::string> splittedPath = PivotUtility::split(childPath, '.');
        const std::string& topNode(splittedPath[0]);
        Datapoint* child = getChild(dp, topNode);
        if (child == nullptr) {
            return static_cast<T>(0);
        }
        // If it is, call recursively this function on the datapoint found with the rest of the path
        splittedPath.erase(splittedPath.begin());
        const std::string& remainingPath(PivotUtility::join(splittedPath, "."));
        return callOnLastPathElement(*child, remainingPath, func);
    }
    else {
        // If last element of the path reached, call function on it
        return func(dp, childPath);
    }
}

static int64_t getIntValue(const Datapoint& dp) {
    return dp.getData().toInt();
}

static std::string getStrValue(const Datapoint& dp) {
    return dp.getData().toStringValue();
}

static bool hasObject(const Reading& reading, const std::string& label) {
    std::vector<Datapoint*> dataPoints = reading.getReadingData();

    for (Datapoint* dp : dataPoints) {
        if (dp->getName() == label) {
            return true;
        }
    }

    return false;
}

static Datapoint* getObject(const Reading& reading, const std::string& label) {
    std::vector<Datapoint*> dataPoints = reading.getReadingData();

    for (Datapoint* dp : dataPoints) {
        if (dp->getName() == label) {
            return dp;
        }
    }

    return nullptr;
}

static void testOutputStream(OUTPUT_HANDLE * handle, READINGSET* readingSet)
{
    const std::vector<Reading*>& readings = readingSet->getAllReadings();

    for (Reading* reading : readings) {
        printf("output: Reading: %s\n", reading->getAssetName().c_str());

        const std::vector<Datapoint*>& datapoints = reading->getReadingData();

        for (Datapoint* dp : datapoints) {
            printf("output:   datapoint: %s -> %s\n", dp->getName().c_str(), dp->getData().toString().c_str());
        }

        lastReading = std::make_shared<Reading>(*reading);
    }

    *(READINGSET **)handle = readingSet;
    outputHandlerCalled++;
}

struct ReadingInfo {
    std::string type;
    std::string value;
};
static void validateReading(std::shared_ptr<Reading> currentReading, const std::string& assetName, const std::string& rootObjectName,
                            const std::vector<std::string>& allAttributeNames, const std::map<std::string, ReadingInfo>& attributes) { 
    ASSERT_NE(nullptr, currentReading.get()) << assetName << ": Invalid reading";
    ASSERT_EQ(assetName, currentReading->getAssetName());
    // Validate data_object structure received
    Datapoint* data_object = nullptr;
    // If no root object name is provided, then create a dummy root object to hold all datapoints
    if (rootObjectName.empty()) {
        std::vector<Datapoint*> datapoints = currentReading->getReadingData();
        ASSERT_NE(datapoints.size(), 0) << assetName << ": Reading contains no data object";
        std::vector<Datapoint*>* rootDatapoints = new std::vector<Datapoint*>;
        DatapointValue dpv(rootDatapoints, true);
        Datapoint* rootDatapoint = new Datapoint("dummyRootDp", dpv);
        std::vector<Datapoint*>* subDatapoints = rootDatapoint->getData().getDpVec();
        subDatapoints->insert(subDatapoints->end(), datapoints.begin(), datapoints.end());
        data_object = rootDatapoint;
    }
    // If root object name is provided, then check that root object is the expected one
    else {
        ASSERT_TRUE(hasObject(*currentReading, rootObjectName)) << assetName << ": " << rootObjectName << " not found";
        data_object = getObject(*currentReading, rootObjectName);
        ASSERT_NE(nullptr, data_object) << assetName << ": " << rootObjectName << " is null";
    }
    // Validate existance of the required keys and non-existance of the others
    for (const auto &kvp : attributes) {
        const std::string& name(kvp.first);
        ASSERT_TRUE(std::find(allAttributeNames.begin(), allAttributeNames.end(), name) != allAttributeNames.end())
            << assetName << ": Attribute not listed in full list: " << name;
    }
    for (const std::string& name : allAttributeNames) {
        bool attributeIsExpected = static_cast<bool>(attributes.count(name));
        std::function<bool(Datapoint&, const std::string&)> hasChildFn(&hasChild);
        ASSERT_EQ(callOnLastPathElement(*data_object, name, hasChildFn),
            attributeIsExpected) << assetName << ": Attribute " << (attributeIsExpected ? "not found: " : "should not exist: ") << name;
    }
    // Validate value and type of each key
    for (auto const& kvp : attributes) {
        const std::string& name = kvp.first;
        const std::string& type = kvp.second.type;
        const std::string& expectedValue = kvp.second.value;
        std::function<Datapoint*(Datapoint&, const std::string&)> getChildFn(&getChild);
        if (type == std::string("string")) {
            ASSERT_EQ(expectedValue, getStrValue(*callOnLastPathElement(*data_object, name, getChildFn))) << assetName << ": Unexpected value for attribute " << name;
        }
        else if (type == std::string("int64_t")) {
            ASSERT_EQ(std::stoll(expectedValue), getIntValue(*callOnLastPathElement(*data_object, name, getChildFn))) << assetName << ": Unexpected value for attribute " << name;
        }
        else if (type == std::string("int64_t_range")) {
            auto splitted = PivotUtility::split(expectedValue, ';');
            ASSERT_EQ(splitted.size(), 2);
            const std::string& expectedRangeMin = splitted.front();
            const std::string& expectedRangeMax = splitted.back();
            int64_t value = getIntValue(*callOnLastPathElement(*data_object, name, getChildFn));
            ASSERT_GE(value, std::stoll(expectedRangeMin)) << assetName << ": Value lower than min for attribute " << name;
            ASSERT_LE(value, std::stoll(expectedRangeMax)) << assetName << ": Value higher than max for attribute " << name;
        }
        else {
            FAIL() << assetName << ": Unknown type: " << type;
        }
    }
}

TEST(PivotHNZPluginIngestRaw, IngestWithNoCallbackDefined)
{
    PLUGIN_HANDLE handle = nullptr;
    ASSERT_NO_THROW(handle = plugin_init(nullptr, nullptr, nullptr));
    ASSERT_TRUE(handle != nullptr);
    HNZPivotFilter* filter = static_cast<HNZPivotFilter*>(handle);

    ASSERT_NO_THROW(plugin_reconfigure(static_cast<PLUGIN_HANDLE*>(handle), test_config));
    ASSERT_EQ(filter->isEnabled(), true);

    std::string jsonMessageTSCE = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":511,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS1", jsonMessageTSCE);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    ASSERT_NO_THROW(plugin_shutdown(reinterpret_cast<PLUGIN_HANDLE*>(filter)));
}

class PivotHNZPluginIngest : public testing::Test
{
protected:
    HNZPivotFilter *filter = nullptr;  // Object on which we call for tests
    ReadingSet *resultReading;

    // Setup is ran for every tests, so each variable are reinitialised
    void SetUp() override
    {
        PLUGIN_HANDLE handle = nullptr;
        ASSERT_NO_THROW(handle = plugin_init(nullptr, &resultReading, testOutputStream));
        filter = static_cast<HNZPivotFilter*>(handle);

        ASSERT_NO_THROW(plugin_reconfigure(static_cast<PLUGIN_HANDLE*>(handle), test_config));
        ASSERT_EQ(filter->isEnabled(), true);

        outputHandlerCalled = 0;
        lastReading = nullptr;
    }

    // TearDown is ran for every tests, so each variable are destroyed again
    void TearDown() override
    {
        if (filter) {
            ASSERT_NO_THROW(plugin_shutdown(reinterpret_cast<PLUGIN_HANDLE*>(filter)));
        }
    }
};

TEST_F(PivotHNZPluginIngest, IngestOnEmptyReadingSet)
{
    ASSERT_NO_THROW(plugin_ingest(filter, nullptr));
    ASSERT_EQ(outputHandlerCalled, 0);
}

TEST_F(PivotHNZPluginIngest, IngestOnPluginDisabled)
{
    static std::string reconfigure = QUOTE({
        "enable": {
            "value": "false"
        }
    });
    
    ASSERT_NO_THROW(plugin_reconfigure(reinterpret_cast<PLUGIN_HANDLE*>(filter), reconfigure));
    ASSERT_EQ(filter->isEnabled(), false);

    std::string jsonMessageTSCE = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":511,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS1", jsonMessageTSCE);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);
}

TEST_F(PivotHNZPluginIngest, OneReadingMultipleDatapoints)
{
    std::string jsonMessagePivot1 = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":511,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    std::string jsonMessagePivot2 = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":511,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    std::string jsonMessagePivot3 = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":511,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS1", {jsonMessagePivot1, jsonMessagePivot2, jsonMessagePivot3});
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    const std::vector<Reading*>& results = resultReading->getAllReadings();
    ASSERT_EQ(results.size(), 1);
    const std::vector<Datapoint*>& datapoints = results[0]->getReadingData();
    int dataPointsFound = datapoints.size();
    ASSERT_EQ(dataPointsFound, 3);
}

TEST_F(PivotHNZPluginIngest, TSCEToPivot)
{
    std::string jsonMessageTSCE = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":511,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS1", jsonMessageTSCE);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(1685019425432);
    validateReading(lastReading, "TS1", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114561"}},
        {"GTIS.Cause.stVal", {"int64_t", "3"}},
        {"GTIS.TmOrg.stVal", {"string", "genuine"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", std::to_string(pivotTimestampPair.first)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", std::to_string(pivotTimestampPair.second)}},
        {"GTIS.TmValidity.stVal", {"string", "good"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TSCGToPivot)
{
    std::string jsonMessageTSCG = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":511,
            "do_value":1,
            "do_valid":0,
            "do_cg":1,
            "do_outdated":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS1", jsonMessageTSCG);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS1", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114561"}},
        {"GTIS.Cause.stVal", {"int64_t", "20"}},
        {"GTIS.TmOrg.stVal", {"string", "substituted"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TSToPivotDouble)
{
    std::string jsonMessageTSCEDouble = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":522,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS2", jsonMessageTSCEDouble);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(1685019425432);
    validateReading(lastReading, "TS2", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114562"}},
        {"GTIS.Cause.stVal", {"int64_t", "3"}},
        {"GTIS.TmOrg.stVal", {"string", "genuine"}},
        {"GTIS.DpsTyp.stVal", {"string", "on"}},
        {"GTIS.DpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.DpsTyp.t.SecondSinceEpoch", {"int64_t", std::to_string(pivotTimestampPair.first)}},
        {"GTIS.DpsTyp.t.FractionOfSecond", {"int64_t", std::to_string(pivotTimestampPair.second)}},
        {"GTIS.TmValidity.stVal", {"string", "good"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TSQualityToPivot)
{
    printf("Testing TSCE with do_valid=1\n");
    std::string jsonMessageTSCEInvalid = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_value":1,
            "do_valid":1,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TS3", jsonMessageTSCEInvalid);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(1685019425432);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "3"}},
        {"GTIS.TmOrg.stVal", {"string", "genuine"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "invalid"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", std::to_string(pivotTimestampPair.first)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", std::to_string(pivotTimestampPair.second)}},
        {"GTIS.TmValidity.stVal", {"string", "good"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TSCE with do_outdated=1\n");
    std::string jsonMessageTSCEOutdated = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":1,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSCEOutdated);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "3"}},
        {"GTIS.TmOrg.stVal", {"string", "genuine"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "questionable"}},
        {"GTIS.SpsTyp.q.DetailQuality.oldData", {"int64_t", "1"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", std::to_string(pivotTimestampPair.first)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", std::to_string(pivotTimestampPair.second)}},
        {"GTIS.TmValidity.stVal", {"string", "good"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TSCE with do_ts_iv=1\n");
    std::string jsonMessageTSCETimestampInvalid = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":1,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSCETimestampInvalid);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "3"}},
        {"GTIS.TmOrg.stVal", {"string", "genuine"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", std::to_string(pivotTimestampPair.first)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", std::to_string(pivotTimestampPair.second)}},
        {"GTIS.TmValidity.stVal", {"string", "invalid"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TSCE with do_ts_c=1\n");
    std::string jsonMessageTSCEChronoLoss = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":1,
            "do_ts_s":0
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSCEChronoLoss);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "3"}},
        {"GTIS.TmOrg.stVal", {"string", "genuine"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "questionable"}},
        {"GTIS.SpsTyp.q.DetailQuality.oldData", {"int64_t", "1"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", std::to_string(pivotTimestampPair.first)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", std::to_string(pivotTimestampPair.second)}},
        {"GTIS.TmValidity.stVal", {"string", "good"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TSCE with do_ts_s=1\n");
    std::string jsonMessageTSCENotSynchro = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_value":1,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":0,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":1
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSCENotSynchro);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "3"}},
        {"GTIS.TmOrg.stVal", {"string", "genuine"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "questionable"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", std::to_string(pivotTimestampPair.first)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", std::to_string(pivotTimestampPair.second)}},
        {"GTIS.SpsTyp.t.TimeQuality.clockNotSynchronized", {"int64_t", "1"}},
        {"GTIS.TmValidity.stVal", {"string", "good"}},
    });
    if(HasFatalFailure()) return;

    
    printf("Testing TSCE with all error flags\n");
    std::string jsonMessageTSCEAllFail = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_value":1,
            "do_valid":1,
            "do_cg":0,
            "do_outdated":1,
            "do_ts": 1685019425432,
            "do_ts_iv":1,
            "do_ts_c":1,
            "do_ts_s":1
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSCEAllFail);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    printf("Testing TS quality update\n");
    std::string jsonMessageTSQualUpdate = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_valid":0,
            "do_cg":0,
            "do_outdated":1,
            "do_ts": 1685019425432,
            "do_ts_iv":0,
            "do_ts_c":0,
            "do_ts_s":0
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSQualUpdate);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "3"}},
        {"GTIS.TmOrg.stVal", {"string", "genuine"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "questionable"}},
        {"GTIS.SpsTyp.q.DetailQuality.oldData", {"int64_t", "1"}},
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t", std::to_string(pivotTimestampPair.first)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t", std::to_string(pivotTimestampPair.second)}},
        {"GTIS.TmValidity.stVal", {"string", "good"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TSCG with do_valid=1\n");
    std::string jsonMessageTSCGInvalid = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_value":1,
            "do_valid":1,
            "do_cg":1,
            "do_outdated":0
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSCGInvalid);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "20"}},
        {"GTIS.TmOrg.stVal", {"string", "substituted"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "invalid"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TSCG with do_outdated=1\n");
    std::string jsonMessageTSCGOutdated = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_value":1,
            "do_valid":0,
            "do_cg":1,
            "do_outdated":1
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSCGOutdated);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "20"}},
        {"GTIS.TmOrg.stVal", {"string", "substituted"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "questionable"}},
        {"GTIS.SpsTyp.q.DetailQuality.oldData", {"int64_t", "1"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TSCG with all error flags\n");
    std::string jsonMessageTSCGAllFail = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_value":1,
            "do_valid":1,
            "do_cg":1,
            "do_outdated":1
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSCGAllFail);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "20"}},
        {"GTIS.TmOrg.stVal", {"string", "substituted"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "invalid"}},
        {"GTIS.SpsTyp.q.DetailQuality.oldData", {"int64_t", "1"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TMAToPivot)
{
    std::string jsonMessageTMA = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_value":-42,
            "do_valid":0,
            "do_an":"TMA",
            "do_outdated":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TM1", jsonMessageTMA);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.mag.i", {"int64_t", "-42"}},
        {"GTIM.MvTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TM8ToPivot)
{
    std::string jsonMessageTMA = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_value":142,
            "do_valid":0,
            "do_an":"TM8",
            "do_outdated":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TM1", jsonMessageTMA);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.mag.i", {"int64_t", "142"}},
        {"GTIM.MvTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TM16ToPivot)
{
    std::string jsonMessageTMA = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_value":-142,
            "do_valid":0,
            "do_an":"TM16",
            "do_outdated":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TM1", jsonMessageTMA);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.mag.i", {"int64_t", "-142"}},
        {"GTIM.MvTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TMQualityToPivot)
{
    printf("Testing TM with do_valid=1\n");
    std::string jsonMessageTMInvalid = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_value":-42,
            "do_valid":1,
            "do_an":"TMA",
            "do_outdated":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TM1", jsonMessageTMInvalid);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.mag.i", {"int64_t", "-42"}},
        {"GTIM.MvTyp.q.Validity", {"string", "invalid"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TM with do_outdated=1\n");
    std::string jsonMessageTMOutdated = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_value":-42,
            "do_valid":0,
            "do_an":"TMA",
            "do_outdated":1
        }
    });
    createReadingSet(readingSet, "TM1", jsonMessageTMOutdated);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.mag.i", {"int64_t", "-42"}},
        {"GTIM.MvTyp.q.Validity", {"string", "questionable"}},
        {"GTIM.MvTyp.q.DetailQuality.oldData", {"int64_t", "1"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TM with all error flags\n");
    std::string jsonMessageTMAllFail = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_value":-42,
            "do_valid":1,
            "do_an":"TMA",
            "do_outdated":1
        }
    });
    createReadingSet(readingSet, "TM1", jsonMessageTMAllFail);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.mag.i", {"int64_t", "-42"}},
        {"GTIM.MvTyp.q.Validity", {"string", "invalid"}},
        {"GTIM.MvTyp.q.DetailQuality.oldData", {"int64_t", "1"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TM quality update\n");
    std::string jsonMessageTMQualUpdate = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_valid":0,
            "do_an":"TMA",
            "do_outdated":1
        }
    });
    createReadingSet(readingSet, "TM1", jsonMessageTMQualUpdate);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.q.Validity", {"string", "questionable"}},
        {"GTIM.MvTyp.q.DetailQuality.oldData", {"int64_t", "1"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TCAckToPivot)
{
    std::string jsonMessageTCAck = QUOTE({
        "data_object":{
            "do_type":"TC",
            "do_station":12,
            "do_addr":142,
            "do_valid":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TC1", jsonMessageTCAck);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TC1", "PIVOT", allPivotAttributeNames, {
        {"GTIC.ComingFrom", {"string", "hnzip"}},
        {"GTIC.Identifier", {"string", "ID222222"}},
        {"GTIC.Cause.stVal", {"int64_t", "7"}},
        {"GTIC.TmOrg.stVal", {"string", "substituted"}},
        {"GTIC.Confirmation.stVal", {"int64_t", "0"}},
        {"GTIC.SpcTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIC.SpcTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIC.SpcTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TCNAckToPivot)
{
    std::string jsonMessageTCAck = QUOTE({
        "data_object":{
            "do_type":"TC",
            "do_station":12,
            "do_addr":142,
            "do_valid":1
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TC1", jsonMessageTCAck);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TC1", "PIVOT", allPivotAttributeNames, {
        {"GTIC.ComingFrom", {"string", "hnzip"}},
        {"GTIC.Identifier", {"string", "ID222222"}},
        {"GTIC.Cause.stVal", {"int64_t", "7"}},
        {"GTIC.TmOrg.stVal", {"string", "substituted"}},
        {"GTIC.Confirmation.stVal", {"int64_t", "1"}},
        {"GTIC.SpcTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIC.SpcTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIC.SpcTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TCAckToPivotDouble)
{
    std::string jsonMessageTCAck = QUOTE({
        "data_object":{
            "do_type":"TC",
            "do_station":12,
            "do_addr":143,
            "do_valid":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TC2", jsonMessageTCAck);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TC2", "PIVOT", allPivotAttributeNames, {
        {"GTIC.ComingFrom", {"string", "hnzip"}},
        {"GTIC.Identifier", {"string", "ID333333"}},
        {"GTIC.Cause.stVal", {"int64_t", "7"}},
        {"GTIC.TmOrg.stVal", {"string", "substituted"}},
        {"GTIC.Confirmation.stVal", {"int64_t", "0"}},
        {"GTIC.DpcTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIC.DpcTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIC.DpcTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TCNAckToPivotDouble)
{
    std::string jsonMessageTCAck = QUOTE({
        "data_object":{
            "do_type":"TC",
            "do_station":12,
            "do_addr":143,
            "do_valid":1
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TC2", jsonMessageTCAck);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TC2", "PIVOT", allPivotAttributeNames, {
        {"GTIC.ComingFrom", {"string", "hnzip"}},
        {"GTIC.Identifier", {"string", "ID333333"}},
        {"GTIC.Cause.stVal", {"int64_t", "7"}},
        {"GTIC.TmOrg.stVal", {"string", "substituted"}},
        {"GTIC.Confirmation.stVal", {"int64_t", "1"}},
        {"GTIC.DpcTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIC.DpcTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIC.DpcTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TVCAckToPivot)
{
    std::string jsonMessageTVCAck = QUOTE({
        "data_object":{
            "do_type":"TVC",
            "do_station":12,
            "do_addr":31,
            "do_value":0,
            "do_valid":0
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TVC1", jsonMessageTVCAck);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TVC1", "PIVOT", allPivotAttributeNames, {
        {"GTIC.ComingFrom", {"string", "hnzip"}},
        {"GTIC.Identifier", {"string", "ID444444"}},
        {"GTIC.Cause.stVal", {"int64_t", "7"}},
        {"GTIC.TmOrg.stVal", {"string", "substituted"}},
        {"GTIC.Confirmation.stVal", {"int64_t", "0"}},
        {"GTIC.IncTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIC.IncTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIC.IncTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, TVCNAckToPivot)
{
    std::string jsonMessageTVCAck = QUOTE({
        "data_object":{
            "do_type":"TVC",
            "do_station":12,
            "do_addr":31,
            "do_value":0,
            "do_valid":1
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "TVC1", jsonMessageTVCAck);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TVC1", "PIVOT", allPivotAttributeNames, {
        {"GTIC.ComingFrom", {"string", "hnzip"}},
        {"GTIC.Identifier", {"string", "ID444444"}},
        {"GTIC.Cause.stVal", {"int64_t", "7"}},
        {"GTIC.TmOrg.stVal", {"string", "substituted"}},
        {"GTIC.Confirmation.stVal", {"int64_t", "1"}},
        {"GTIC.IncTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIC.IncTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIC.IncTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, PivotToTC)
{
    std::string jsonMessagePivotTC = QUOTE({
        "PIVOT": {
            "GTIC": {
                "SpcTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714185
                    },
                    "ctlVal": 1
                },
                "Identifier": "ID222222",
                "TmOrg": {
                    "stVal": "substituted"
                }
            }
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "PivotCommand", jsonMessagePivotTC);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "HNZCommand", "", allCommandAttributeNames, {
        {"co_type", {"string", "TC"}},
        {"co_addr", {"int64_t", "142"}},
        {"co_value", {"int64_t", "1"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, PivotDoubleToTC)
{
    std::string jsonMessagePivotTCDouble = QUOTE({
        "PIVOT": {
            "GTIC": {
                "DpcTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714185
                    },
                    "ctlVal": "off"
                },
                "Identifier": "ID333333",
                "TmOrg": {
                    "stVal": "substituted"
                }
            }
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "PivotCommand", jsonMessagePivotTCDouble);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "HNZCommand", "", allCommandAttributeNames, {
        {"co_type", {"string", "TC"}},
        {"co_addr", {"int64_t", "143"}},
        {"co_value", {"int64_t", "2"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, PivotToTVC)
{
    std::string jsonMessagePivotTVC = QUOTE({
        "PIVOT": {
            "GTIC": {
                "IncTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714185
                    },
                    "ctlVal": 42
                },
                "Identifier": "ID444444",
                "TmOrg": {
                    "stVal": "substituted"
                }
            }
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "PivotCommand", jsonMessagePivotTVC);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "HNZCommand", "", allCommandAttributeNames, {
        {"co_type", {"string", "TVC"}},
        {"co_addr", {"int64_t", "31"}},
        {"co_value", {"int64_t", "42"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, SouthEvent)
{
    std::string jsonMessageSouthEvent = QUOTE({
        "south_event":{
            "connx_status": "not connected",
            "gi_status": "idle"
        }
    });
    ReadingSet* readingSet = nullptr;
    createReadingSet(readingSet, "CONNECTION-1", jsonMessageSouthEvent);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "CONNECTION-1", "south_event", allSouthEventAttributeNames, {
        {"connx_status", {"string", "not connected"}},
        {"gi_status", {"string", "idle"}},
    });
    if(HasFatalFailure()) return;
}

TEST_F(PivotHNZPluginIngest, InvalidMessages)
{
    printf("Testing message with no datapoint\n");
    ReadingSet* readingSet = nullptr;
    createEmptyReadingSet(readingSet, "INVALID");
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    printf("Testing message with unknown root object\n");
    std::string jsonMessageUnknownRoot = QUOTE({
        "unknown_message":{
            "val": 42
        }
    });
    createReadingSet(readingSet, "UNKNOWN", jsonMessageUnknownRoot);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    outputHandlerCalled = 0;
    validateReading(lastReading, "UNKNOWN", "unknown_message", {"val"}, {
        {"val", {"int64_t", "42"}},
    });

    printf("Testing data_object message with missing do_type\n");
    std::string jsonMessageMissingType = QUOTE({
        "data_object":{
            "do_station":12,
            "do_addr":31,
            "do_value":0,
            "do_valid":0
        }
    });
    createReadingSet(readingSet, "PivotCommand", jsonMessageMissingType);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    printf("Testing data_object message with missing do_addr\n");
    std::string jsonMessageMissingAddress = QUOTE({
        "data_object":{
            "do_type":"TVC",
            "do_station":12,
            "do_value":0,
            "do_valid":0
        }
    });
    createReadingSet(readingSet, "PivotCommand", jsonMessageMissingAddress);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    printf("Testing data_object message with missing pivot id for given type and address\n");
    std::string jsonMessageUnknownPivotId = QUOTE({
        "data_object":{
            "do_type":"TVC",
            "do_station":12,
            "do_addr":1,
            "do_value":0,
            "do_valid":0
        }
    });
    createReadingSet(readingSet, "PivotCommand", jsonMessageUnknownPivotId);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    printf("Testing PIVOT message with missing/invalid root type\n");
    std::string jsonMessageInvalidPivotRootType = QUOTE({
        "PIVOT": {
            "GTIX": {
                "SpcTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714185
                    },
                    "ctlVal": 1
                },
                "Identifier": "ID222222",
                "TmOrg": {
                    "stVal": "substituted"
                }
            }
        }
    });
    createReadingSet(readingSet, "PivotCommand", jsonMessageInvalidPivotRootType);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    printf("Testing PIVOT message with missing pivot type\n");
    std::string jsonMessageMissingPivotType = QUOTE({
        "PIVOT": {
            "GTIC": {
                "Identifier": "ID222222",
                "TmOrg": {
                    "stVal": "substituted"
                }
            }
        }
    });
    createReadingSet(readingSet, "PivotCommand", jsonMessageMissingPivotType);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    printf("Testing PIVOT message with invalid pivot type\n");
    std::string jsonMessageInvalidPivotType = QUOTE({
        "PIVOT": {
            "GTIC": {
                "SpsTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714185
                    },
                    "ctlVal": 1
                },
                "Identifier": "ID222222",
                "TmOrg": {
                    "stVal": "substituted"
                }
            }
        }
    });
    createReadingSet(readingSet, "PivotCommand", jsonMessageInvalidPivotType);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    printf("Testing PIVOT message with missing Identifier\n");
    std::string jsonMessageMissingId = QUOTE({
        "PIVOT": {
            "GTIC": {
                "SpcTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714185
                    },
                    "ctlVal": 1
                },
                "TmOrg": {
                    "stVal": "substituted"
                }
            }
        }
    });
    createReadingSet(readingSet, "PivotCommand", jsonMessageMissingId);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    printf("Testing PIVOT message with unknown Identifier\n");
    std::string jsonMessageUnknownId = QUOTE({
        "PIVOT": {
            "GTIC": {
                "SpcTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714185
                    },
                    "ctlVal": 1
                },
                "Identifier": "ID000000",
                "TmOrg": {
                    "stVal": "substituted"
                }
            }
        }
    });
    createReadingSet(readingSet, "PivotCommand", jsonMessageUnknownId);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 0);

    // Cases below only generates a warning and still process the reading

    printf("Testing data_object label mismatch\n");
    std::string jsonMessageLabelMismatch = QUOTE({
        "data_object":{
            "do_type":"TVC",
            "do_station":12,
            "do_addr":31,
            "do_value":0,
            "do_valid":0
        }
    });
    createReadingSet(readingSet, "TVC42", jsonMessageLabelMismatch);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    auto pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    long sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TVC42", "PIVOT", allPivotAttributeNames, {
        {"GTIC.ComingFrom", {"string", "hnzip"}},
        {"GTIC.Identifier", {"string", "ID444444"}},
        {"GTIC.Cause.stVal", {"int64_t", "7"}},
        {"GTIC.TmOrg.stVal", {"string", "substituted"}},
        {"GTIC.Confirmation.stVal", {"int64_t", "0"}},
        {"GTIC.IncTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIC.IncTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIC.IncTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TS with missing attributes\n");
    std::string jsonMessageTSInvalid = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_addr":577
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSInvalid);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "3"}},
        {"GTIS.TmOrg.stVal", {"string", "substituted"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TSCE with missing CE attributes\n");
    std::string jsonMessageTSCEMissingAttributes = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_addr":577,
            "do_cg":0
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSCEMissingAttributes);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "3"}},
        {"GTIS.TmOrg.stVal", {"string", "substituted"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TS with value out of range\n");
    std::string jsonMessageTSValueOutOfRange = QUOTE({
        "data_object":{
            "do_type":"TS",
            "do_station":12,
            "do_addr":577,
            "do_value":-1,
            "do_valid":0,
            "do_cg":1,
            "do_outdated":0
        }
    });
    createReadingSet(readingSet, "TS3", jsonMessageTSValueOutOfRange);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TS3", "PIVOT", allPivotAttributeNames, {
        {"GTIS.ComingFrom", {"string", "hnzip"}},
        {"GTIS.Identifier", {"string", "ID114567"}},
        {"GTIS.Cause.stVal", {"int64_t", "20"}},
        {"GTIS.TmOrg.stVal", {"string", "substituted"}},
        {"GTIS.SpsTyp.stVal", {"int64_t", "1"}},
        {"GTIS.SpsTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIS.SpsTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIS.SpsTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TM with missing attributes\n");
    std::string jsonMessageTMAMissingAttributes = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_addr":20
        }
    });
    createReadingSet(readingSet, "TM1", jsonMessageTMAMissingAttributes);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TM with invalid do_an\n");
    std::string jsonMessageTMInvalidType = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_value":999999,
            "do_valid":0,
            "do_an":"TMT",
            "do_outdated":0
        }
    });
    createReadingSet(readingSet, "TM1", jsonMessageTMInvalidType);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.mag.i", {"int64_t", "999999"}},
        {"GTIM.MvTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TMA with value out of range\n");
    std::string jsonMessageTMAValueOutOfRange = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_value":142,
            "do_valid":0,
            "do_an":"TMA",
            "do_outdated":0
        }
    });
    createReadingSet(readingSet, "TM1", jsonMessageTMAValueOutOfRange);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.mag.i", {"int64_t", "142"}},
        {"GTIM.MvTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TM8 with value out of range\n");
    std::string jsonMessageTM8ValueOutOfRange = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_value":-1,
            "do_valid":0,
            "do_an":"TMA",
            "do_outdated":0
        }
    });
    createReadingSet(readingSet, "TM1", jsonMessageTM8ValueOutOfRange);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.mag.i", {"int64_t", "-1"}},
        {"GTIM.MvTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TM16 with value out of range\n");
    std::string jsonMessageTM16ValueOutOfRange = QUOTE({
        "data_object":{
            "do_type":"TM",
            "do_station":12,
            "do_addr":20,
            "do_value":999999,
            "do_valid":0,
            "do_an":"TMA",
            "do_outdated":0
        }
    });
    createReadingSet(readingSet, "TM1", jsonMessageTM16ValueOutOfRange);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TM1", "PIVOT", allPivotAttributeNames, {
        {"GTIM.ComingFrom", {"string", "hnzip"}},
        {"GTIM.Identifier", {"string", "ID111111"}},
        {"GTIM.Cause.stVal", {"int64_t", "1"}},
        {"GTIM.TmOrg.stVal", {"string", "substituted"}},
        {"GTIM.MvTyp.mag.i", {"int64_t", "999999"}},
        {"GTIM.MvTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIM.MvTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIM.MvTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TC ACK with missing attributes\n");
    std::string jsonMessageTCAck = QUOTE({
        "data_object":{
            "do_type":"TC",
            "do_addr":142
        }
    });
    createReadingSet(readingSet, "TC1", jsonMessageTCAck);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TC1", "PIVOT", allPivotAttributeNames, {
        {"GTIC.ComingFrom", {"string", "hnzip"}},
        {"GTIC.Identifier", {"string", "ID222222"}},
        {"GTIC.Cause.stVal", {"int64_t", "7"}},
        {"GTIC.TmOrg.stVal", {"string", "substituted"}},
        {"GTIC.Confirmation.stVal", {"int64_t", "0"}},
        {"GTIC.SpcTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIC.SpcTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIC.SpcTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing TVC ACK with missing attributes\n");
    std::string jsonMessageTVCAck = QUOTE({
        "data_object":{
            "do_type":"TVC",
            "do_addr":31
        }
    });
    createReadingSet(readingSet, "TVC1", jsonMessageTVCAck);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    pivotTimestampPair = PivotTimestamp::fromTimestamp(PivotTimestamp::getCurrentTimestampMs());
    sec = pivotTimestampPair.first;
    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TVC1", "PIVOT", allPivotAttributeNames, {
        {"GTIC.ComingFrom", {"string", "hnzip"}},
        {"GTIC.Identifier", {"string", "ID444444"}},
        {"GTIC.Cause.stVal", {"int64_t", "7"}},
        {"GTIC.TmOrg.stVal", {"string", "substituted"}},
        {"GTIC.Confirmation.stVal", {"int64_t", "0"}},
        {"GTIC.IncTyp.q.Validity", {"string", "good"}},
        // NB: Time was added by hnztopivot plugin
        {"GTIC.IncTyp.t.SecondSinceEpoch", {"int64_t_range", std::to_string(sec) + ";" + std::to_string(sec+1)}},
        {"GTIC.IncTyp.t.FractionOfSecond", {"int64_t_range", "0;99999999"}},
    });
    if(HasFatalFailure()) return;

    printf("Testing PIVOT label mismatch\n");
    std::string jsonMessagePivotLabelMismatch = QUOTE({
        "PIVOT": {
            "GTIC": {
                "IncTyp": {
                    "q": {
                        "Source": "process",
                        "Validity": "good"
                    },
                    "t": {
                        "FractionOfSecond": 9529458,
                        "SecondSinceEpoch": 1669714185
                    },
                    "ctlVal": 42
                },
                "Identifier": "ID444444",
                "TmOrg": {
                    "stVal": "substituted"
                }
            }
        }
    });
    createReadingSet(readingSet, "TVC42", jsonMessagePivotLabelMismatch);
    if(HasFatalFailure()) return;
    ASSERT_NE(readingSet, nullptr);

    outputHandlerCalled = 0;
    ASSERT_NO_THROW(plugin_ingest(filter, static_cast<READINGSET*>(readingSet)));
    ASSERT_EQ(outputHandlerCalled, 1);
    validateReading(lastReading, "TVC42", "", allCommandAttributeNames, {
        {"co_type", {"string", "TVC"}},
        {"co_addr", {"int64_t", "31"}},
        {"co_value", {"int64_t", "42"}},
    });
    if(HasFatalFailure()) return;
}
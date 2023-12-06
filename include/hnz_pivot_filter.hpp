/*
 * FledgePower IEC 104 <-> pivot filter plugin.
 *
 * Copyright (c) 2022, RTE (https://www.rte-france.com)
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Michael Zillgith (michael.zillgith at mz-automation.de)
 * 
 */

#ifndef _HNZ_PIVOT_FILTER_H
#define _HNZ_PIVOT_FILTER_H

#include <string>
#include <mutex>
#include <filter.h>
#include <config_category.h>

class Datapoint;
class HNZPivotConfig;
class HNZPivotDataPoint;

class HNZPivotFilter : public FledgeFilter {

public:
    /*
     * Struct used to store fields of a data object during processing
    */
    struct GenericDataObject {
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
    };

    /**
     * Constructor for the HNZPivotFilter.
     *
     * We call the constructor of the base class and handle the initial
     * configuration of the filter.
     *
     * @param    filterName      The name of the filter
     * @param    filterConfig    The configuration category for this filter
     * @param    outHandle       The handle of the next filter in the chain
     * @param    output          A function pointer to call to output data to the next filter
     */
    HNZPivotFilter(const std::string& filterName,
        ConfigCategory& filterConfig,
        OUTPUT_HANDLE* outHandle,
        OUTPUT_STREAM output);

    /**
     * The actual filtering code
     *
     * @param readingSet The reading data to filter
     */
    void ingest(READINGSET* readingSet);

    /**
     * Reconfiguration entry point to the filter.
     *
     * This method runs holding the configMutex to prevent
     * ingest using the regex class that may be destroyed by this
     * call.
     *
     * Pass the configuration to the base FilterPlugin class and
     * then call the private method to handle the filter specific
     * configuration.
     *
     * @param newConfig  The JSON of the new configuration
     */
    void reconfigure(const std::string& newConfig);

private:
    void readConfig(const ConfigCategory& config);

    Datapoint* addElement(Datapoint* dp, const std::string& elementPath);

    template <class T>
    Datapoint* addElementWithValue(Datapoint* dp, const std::string& elementPath, const T value);

    Datapoint* createDp(const std::string& name);

    template <class T>
    Datapoint* createDpWithValue(const std::string& name, const T value);

    bool convertDatapoint(const std::string& assetName, Datapoint* dp, std::vector<Datapoint*>& convertedDatapoints);

    template <typename T>
    void static readAttribute(std::map<std::string, bool>& attributeFound, Datapoint* dp,
                              const std::string& targetName, T& out);
    void static readAttribute(std::map<std::string, bool>& attributeFound, Datapoint* dp,
                              const std::string& targetName, Datapoint*& out);
    void static readAttribute(std::map<std::string, bool>& attributeFound, Datapoint* dp,
                              const std::string& targetName, std::string& out);
    Datapoint* convertDatapointToPivot(const std::string& assetName, Datapoint* sourceDp);
    Datapoint* convertTSToPivot(const std::string& assetName, std::map<std::string, bool>& attributeFound,
                                const GenericDataObject& dataObject, std::shared_ptr<HNZPivotDataPoint> exchangeConfig);
    Datapoint* convertTMToPivot(const std::string& assetName, std::map<std::string, bool>& attributeFound,
                                const GenericDataObject& dataObject, std::shared_ptr<HNZPivotDataPoint> exchangeConfig);
    Datapoint* convertTCACKToPivot(const std::string& assetName, std::map<std::string, bool>& attributeFound,
                                   const GenericDataObject& dataObject, std::shared_ptr<HNZPivotDataPoint> exchangeConfig);
    Datapoint* convertTVCACKToPivot(const std::string& assetName, std::map<std::string, bool>& attributeFound,
                                    const GenericDataObject& dataObject, std::shared_ptr<HNZPivotDataPoint> exchangeConfig);

    std::vector<Datapoint*> convertDatapointToHNZ(const std::string& assetName, Datapoint* sourceDp);

    std::shared_ptr<HNZPivotConfig> m_filterConfig;
    std::recursive_mutex            m_configMutex;
};


#endif /* _HNZ_PIVOT_FILTER_H */
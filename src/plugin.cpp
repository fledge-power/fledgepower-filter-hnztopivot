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

#include <version.h>
#include <plugin_api.h>
#include <logger.h>

#include "hnz_pivot_utility.hpp"
#include "hnz_pivot_filter.hpp"
#include "hnz_pivot_filter_config.hpp"

extern "C" {

/**
 * Plugin specific default configuration
 */
static const char *default_config = QUOTE({
    "plugin" : {
        "description" : "HNZ to pivot filter plugin",
        "type" : "string",
        "default" : FILTER_NAME,
        "readonly" : "true"
    },
    "enable": {
        "description": "A switch that can be used to enable or disable execution of the filter.",
        "displayName": "Enabled",
        "type": "boolean",
        "default": "true"
    },
    "exchanged_data": {
        "description" : "exchanged data list",
        "type" : "JSON",
        "displayName" : "Exchanged data list",
        "order" : "1",
        "default" : QUOTE({
            "exchanged_data" : {
                "name" : "SAMPLE",
                "version" : "1.0",
                "datapoints" : [
                    {
                        "label" : "TS1",
                        "pivot_id" : "ID114562",
                        "pivot_type" : "SpsTyp",
                        "protocols" : [
                            {
                                "name" : "iec104",
                                "address" : "45-672",
                                "typeid" : "M_SP_TB_1"
                            },
                            {
                                "name" : "tase2",
                                "address" : "S_114562",
                                "typeid" : "Data_StateQTimeTagExtended"
                            },
                            {
                                "name" : "hnzip",
                                "address" : "511",
                                "station" : "12",
                                "typeid" : "TS"
                            }
                        ]
                    },
                    {
                        "label" : "TM1",
                        "pivot_id" : "ID99876",
                        "pivot_type" : "MvTyp",
                        "protocols" : [
                            {
                                "name" : "iec104",
                                "address" : "45-984",
                                "typeid" : "M_ME_NA_1"
                            },
                            {
                                "name" : "tase2",
                                "address" : "S_114563",
                                "typeid" : "Data_RealQ"
                            },
                            {
                                "name" : "hnzip",
                                "address" : "512",
                                "station" : "12",
                                "typeid" : "TM"
                            }
                        ]
                    }
                ]
            }
        })
    }
});


/**
 * The C API plugin information structure
 */
static PLUGIN_INFORMATION pluginInfo = {
	   FILTER_NAME,			// Name
	   VERSION,			    // Version
	   0,		            // Flags
	   PLUGIN_TYPE_FILTER,	// Type
	   "1.0.0",			    // Interface version
	   default_config		// Configuration
};

/**
 * Return the information about this plugin
 */
PLUGIN_INFORMATION *plugin_info()
{
	return &pluginInfo;
}

/**
 * Initialise the plugin with configuration.
 *
 * This function is called to get the plugin handle.
 */
PLUGIN_HANDLE plugin_init(ConfigCategory* config,
                          OUTPUT_HANDLE *outHandle,
                          OUTPUT_STREAM output)
{
    std::string beforeLog = HNZPivotConfig::getPluginName() + " -";
    HnzPivotUtility::log_info("%s Initializing filter", beforeLog.c_str());

    if (config == nullptr) {
        HnzPivotUtility::log_warn("%s No config provided for filter, using default config", beforeLog.c_str());
        auto info = plugin_info();
	    config = new ConfigCategory("hnztopivot", info->config);
        config->setItemsValueFromDefault();
    }
    
    auto pivotFilter = new HNZPivotFilter(FILTER_NAME,
                                *config, outHandle, output);

	return static_cast<PLUGIN_HANDLE>(pivotFilter);
}

/**
 * Ingest a set of readings into the plugin for processing
 *
 * @param handle     The plugin handle returned from plugin_init
 * @param readingSet The readings to process
 */
void plugin_ingest(PLUGIN_HANDLE handle,
                   READINGSET *readingSet)
{
    auto pivotFilter = static_cast<HNZPivotFilter*>(handle);
    pivotFilter->ingest(readingSet);
}

/**
 * Plugin reconfiguration method
 *
 * @param handle     The plugin handle
 * @param newConfig  The updated configuration
 */
void plugin_reconfigure(PLUGIN_HANDLE handle, const std::string& newConfig)
{
    auto pivotFilter = static_cast<HNZPivotFilter*>(handle);
    pivotFilter->reconfigure(newConfig);
}

/**
 * Call the shutdown method in the plugin
 */
void plugin_shutdown(PLUGIN_HANDLE handle)
{
    auto pivotFilter = static_cast<HNZPivotFilter*>(handle);
    delete pivotFilter;
}

// End of extern "C"
};
